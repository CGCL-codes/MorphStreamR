package durability.logging.LoggingStrategy.ImplLoggingManager;

import common.collections.Configuration;
import common.collections.OsUtils;
import common.io.ByteIO.DataInputView;
import common.io.ByteIO.InputWithDecompression.NativeDataInputView;
import common.io.ByteIO.InputWithDecompression.SnappyDataInputView;
import durability.ftmanager.FTManager;
import durability.logging.LoggingEntry.PathRecord;
import durability.logging.LoggingResource.ImplLoggingResources.DependencyMaintainResources;
import durability.logging.LoggingResult.Attachment;
import durability.logging.LoggingResult.LoggingHandler;
import durability.logging.LoggingStrategy.LoggingManager;
import durability.logging.LoggingStream.ImplLoggingStreamFactory.NIOPathStreamFactory;
import durability.recovery.RedoLogResult;
import durability.recovery.histroyviews.HistoryViews;
import durability.snapshot.LoggingOptions;
import durability.struct.Logging.LoggingEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.table.RecordSchema;
import utils.lib.ConcurrentHashMap;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.nio.file.StandardOpenOption.READ;
import static utils.FaultToleranceConstants.CompressionType.None;

public class PathLoggingManager implements LoggingManager {
    private static final Logger LOG = LoggerFactory.getLogger(PathLoggingManager.class);
    @Nonnull protected String loggingPath;
    @Nonnull protected LoggingOptions loggingOptions;
    protected int parallelNum;
    public ConcurrentHashMap<Integer, PathRecord> threadToPathRecord = new ConcurrentHashMap<>();
    public HistoryViews historyViews = new HistoryViews();//Used when recovery
    public PathLoggingManager(Configuration configuration) {
        loggingPath = configuration.getString("rootFilePath") + OsUtils.OS_wrapper("logging");
        parallelNum = configuration.getInt("parallelNum");
        loggingOptions = new LoggingOptions(parallelNum, configuration.getString("compressionAlg"));
        for (int i = 0; i < parallelNum; i ++) {
            this.threadToPathRecord.put(i, new PathRecord());
        }
    }
    public DependencyMaintainResources syncPrepareResource(int partitionId) {
        return new DependencyMaintainResources(partitionId, this.threadToPathRecord.get(partitionId));
    }

    @Override
    public void registerTable(RecordSchema recordSchema, String tableName) {
        throw new UnsupportedOperationException("Not supported yet");
    }

    @Override
    public void addLogRecord(LoggingEntry logRecord) {
        throw new UnsupportedOperationException("Not supported yet");
    }

    @Override
    public void commitLog(long groupId, int partitionId, FTManager ftManager) throws IOException {
        NIOPathStreamFactory nioPathStreamFactory = new NIOPathStreamFactory(this.loggingPath);
        DependencyMaintainResources dependencyMaintainResources = syncPrepareResource(partitionId);
        AsynchronousFileChannel afc = nioPathStreamFactory.createLoggingStream();
        Attachment attachment = new Attachment(nioPathStreamFactory.getPath(), groupId, partitionId, afc, ftManager);
        ByteBuffer dataBuffer = dependencyMaintainResources.createWriteBuffer(loggingOptions);
        afc.write(dataBuffer, 0, attachment, new LoggingHandler());
    }

    @Override
    public void syncRetrieveLogs(RedoLogResult redoLogResult) throws IOException, ExecutionException, InterruptedException {
        //Construct the history views
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
            String[] abortIds = strings[0].split(";");
            for (String abortId : abortIds) {
                this.historyViews.addAbortId(redoLogResult.threadId, Long.parseLong(abortId));
            }
            for (int j = 1; j < strings.length; j++) {
                String[] dependency = strings[j].split(";");
                String tableName = dependency[0];
                for (int k = 1; k < dependency.length; k++) {
                    String[] kp = dependency[k].split(":");
                    String key = kp[0];
                    for (int l = 1; l < kp.length; l++) {
                        String[] pr = kp[l].split(",");
                        String p = pr[0];
                        for (int m = 1; m < pr.length; m++) {
                            String[] kv = pr[m].split("/");
                            this.historyViews.addDependencies(redoLogResult.groupIds.get(i), tableName, key, p, Long.parseLong(kv[0]), kv[1]);
                        }
                    }
                }
            }
            LOG.info("Finish construct the history views");
        }
    }

    @Override
    public boolean inspectAbortView(long bid) {
        return this.historyViews.inspectAbortView(bid);
    }

    @Override
    public Object inspectDependencyView(long groupId, String table, String from, String to, long bid) {
        return this.historyViews.inspectDependencyView(table, from, to, bid);
    }

    @Override
    public HistoryViews getHistoryViews() {
        return this.historyViews;
    }

    public static Logger getLOG() {
        return LOG;
    }
}
