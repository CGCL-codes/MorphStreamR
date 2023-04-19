package durability.logging.LoggingStrategy.ImplLoggingManager;

import common.collections.Configuration;
import common.collections.OsUtils;
import common.io.ByteIO.DataInputView;
import common.io.ByteIO.InputWithDecompression.NativeDataInputView;
import common.io.ByteIO.InputWithDecompression.SnappyDataInputView;
import common.util.io.IOUtils;
import durability.ftmanager.FTManager;
import durability.logging.LoggingResource.ImplLoggingResources.CommandLoggingResources;
import durability.logging.LoggingResult.Attachment;
import durability.logging.LoggingResult.LoggingHandler;
import durability.logging.LoggingStrategy.LoggingManager;
import durability.logging.LoggingStream.ImplLoggingStreamFactory.NIOCommandStreamFactory;
import durability.recovery.RedoLogResult;
import durability.recovery.command.CommandPrecedenceGraph;
import durability.recovery.histroyviews.HistoryViews;
import durability.snapshot.LoggingOptions;
import durability.struct.Logging.CommandLog;
import durability.struct.Logging.LoggingEntry;
import durability.struct.Logging.NativeCommandLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.SchemaRecord;
import storage.TableRecord;
import storage.datatype.DataBox;
import storage.table.BaseTable;
import storage.table.RecordSchema;
import transaction.function.DEC;
import transaction.function.INC;
import utils.AppConfig;
import utils.SOURCE_CONTROL;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.nio.file.StandardOpenOption.READ;
import static utils.FaultToleranceConstants.CompressionType.None;

public class CommandLoggingManager implements LoggingManager {
    private static final Logger LOG = LoggerFactory.getLogger(CommandLoggingManager.class);
    @Nonnull
    protected String loggingPath;
    @Nonnull protected LoggingOptions loggingOptions;
    protected int parallelNum;
    protected final int num_items;
    protected final int delta;
    protected Map<String, BaseTable> tables;
    protected int app;
    protected ConcurrentHashMap<Integer, Vector<NativeCommandLog>> threadToCommandLog = new ConcurrentHashMap<>();
    protected CommandPrecedenceGraph cpg = new CommandPrecedenceGraph();
    public CommandLoggingManager(Map<String, BaseTable> tables, Configuration configuration) {
        this.tables = tables;
        loggingPath = configuration.getString("rootFilePath") + OsUtils.OS_wrapper("logging");
        parallelNum = configuration.getInt("parallelNum");
        loggingOptions = new LoggingOptions(parallelNum, configuration.getString("compressionAlg"));
        num_items = configuration.getInt("NUM_ITEMS");
        app = configuration.getInt("app");
        delta = num_items / parallelNum;
        for (int i = 0; i < parallelNum; i++) {
            threadToCommandLog.put(i, new Vector<>());
        }
    }
    public CommandLoggingResources syncPrepareResource(int partitionId) {
        return new CommandLoggingResources(partitionId, threadToCommandLog.get(partitionId));
    }
    @Override
    public void addLogRecord(LoggingEntry logRecord) {
        NativeCommandLog nativeCommandLog = (NativeCommandLog) logRecord;
        if (!this.threadToCommandLog.get(getPartitionId(nativeCommandLog.key)).contains(nativeCommandLog)) {
            this.threadToCommandLog.get(getPartitionId(nativeCommandLog.key)).add(nativeCommandLog);
        }
    }
    @Override
    public void commitLog(long groupId, int partitionId, FTManager ftManager) throws IOException {
        NIOCommandStreamFactory commandStreamFactory = new NIOCommandStreamFactory(loggingPath);
        CommandLoggingResources commandLoggingResources = syncPrepareResource(partitionId);
        AsynchronousFileChannel afc = commandStreamFactory.createLoggingStream();
        Attachment attachment = new Attachment(commandStreamFactory.getPath(), groupId, partitionId, afc, ftManager);
        ByteBuffer dataBuffer = commandLoggingResources.createWriteBuffer(loggingOptions);
        afc.write(dataBuffer, 0, attachment, new LoggingHandler());
    }
    @Override
    public void syncRetrieveLogs(RedoLogResult redoLogResult) throws IOException, ExecutionException, InterruptedException {
        for (int i = 0; i < redoLogResult.redoLogPaths.size(); i++) {
            Path walPath = Paths.get(redoLogResult.redoLogPaths.get(i));
            AsynchronousFileChannel afc = AsynchronousFileChannel.open(walPath, READ);
            int fileSize = (int) afc.size();
            ByteBuffer dataBuffer = ByteBuffer.allocate(fileSize);
            Future<Integer> result = afc.read(dataBuffer, 0);
            result.get();
            DataInputView inputView;
            if (loggingOptions.getCompressionAlg() != None) {
                inputView = new SnappyDataInputView(dataBuffer);//Default to use Snappy compression
            } else {
                inputView = new NativeDataInputView(dataBuffer);
            }
            byte[] object = inputView.readFullyDecompression();
            String[] strings = new String(object, StandardCharsets.UTF_8).split(" ");
            for (String log : strings) {
                NativeCommandLog nativeCommandLog = NativeCommandLog.getNativeCommandLog(log);
                this.cpg.addTask(nativeCommandLog);
            }
            LOG.info("Thread " + redoLogResult.threadId + " has finished reading logs");
            SOURCE_CONTROL.getInstance().waitForOtherThreads(redoLogResult.threadId);
            if (redoLogResult.threadId == 0) {
                IOUtils.println("Total number of tasks: " + this.cpg.tasks.size());
                start_evaluate();
            }
            SOURCE_CONTROL.getInstance().waitForOtherThreads(redoLogResult.threadId);
        }
    }
    private void start_evaluate () {
        for (NativeCommandLog nativeCommandLog : this.cpg.tasks) {
            PROCESS(nativeCommandLog);
        }
        this.cpg.tasks.clear();
    }
    private void PROCESS(NativeCommandLog nativeCommandLog) {
        switch (app) {
            case 0:
            case 3:
            case 2:
                break;
            case 1:
                SLExecute(nativeCommandLog);
                break;
        }
    }
    @Override
    public void registerTable(RecordSchema recordSchema, String tableName) {
        throw new UnsupportedOperationException();
    }
    public int getPartitionId(String primary_key) {
        int key = Integer.parseInt(primary_key);
        return key / delta;
    }

    @Override
    public boolean inspectAbortView(long groupId, int threadId, long bid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int inspectAbortNumber(long groupId, int threadId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object inspectDependencyView(long groupId, String table, String from, String to, long bid) {
       throw new UnsupportedOperationException();
    }

    @Override
    public HashMap<String, List<Integer>> inspectTaskPlacing(long groupId, int threadId) {
        throw  new UnsupportedOperationException();
    }

    @Override
    public HistoryViews getHistoryViews() {
        throw new UnsupportedOperationException();
    }
    private void SLExecute(NativeCommandLog task) {
        if (task == null) return;
        String table = task.tableName;
        String pKey = task.key;
        double value = Double.parseDouble(task.id);
        long bid = (long) Math.floor(value);
        if (task.condition.length > 0) {
            SchemaRecord preValue = this.tables.get(table).SelectKeyRecord(task.condition[0]).content_.readPreValues(bid);
            long sourceAccountBalance = preValue.getValues().get(1).getLong();
            AppConfig.randomDelay();
            SchemaRecord srcRecord = this.tables.get(table).SelectKeyRecord(pKey).record_;
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record
            if (task.OperationFunction.equals(INC.class.getName())) {
                tempo_record.getValues().get(1).incLong(sourceAccountBalance, Long.parseLong(task.parameter));//compute.
            } else if (task.OperationFunction.equals(DEC.class.getName())) {
                tempo_record.getValues().get(1).decLong(sourceAccountBalance, Long.parseLong(task.parameter));//compute.
            }
            this.tables.get(table).SelectKeyRecord(pKey).content_.updateMultiValues(bid, 0, false, tempo_record);
        } else {
            TableRecord src = this.tables.get(table).SelectKeyRecord(pKey);
            SchemaRecord srcRecord = src.content_.readPreValues(bid);
            List<DataBox> values = srcRecord.getValues();
            AppConfig.randomDelay();
            SchemaRecord tempo_record;
            tempo_record = new SchemaRecord(values);//tempo record
            tempo_record.getValues().get(1).incLong(Long.parseLong(task.parameter));//compute.
            src.content_.updateMultiValues(bid, 0, false, tempo_record);
        }
    }
}
