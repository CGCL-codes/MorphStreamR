package durability.logging.LoggingStrategy.ImplLoggingManager;

import common.collections.Configuration;
import common.collections.OsUtils;
import durability.ftmanager.FTManager;
import durability.logging.LoggingEntry.LVLogRecord;
import durability.logging.LoggingResource.ImplLoggingResources.LSNVectorLoggingResources;
import durability.logging.LoggingResult.Attachment;
import durability.logging.LoggingResult.LoggingHandler;
import durability.logging.LoggingStrategy.LoggingManager;
import durability.logging.LoggingStream.ImplLoggingStreamFactory.NIOLSNVectorStreamFactory;
import durability.recovery.RedoLogResult;
import durability.recovery.histroyviews.HistoryViews;
import durability.snapshot.LoggingOptions;
import durability.struct.Logging.LVCLog;
import durability.struct.Logging.LoggingEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.TableRecord;
import storage.table.BaseTable;
import storage.table.RecordSchema;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.HashMap;
import java.util.List;
import utils.lib.ConcurrentHashMap;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class LSNVectorLoggingManager implements LoggingManager {
    private static final Logger LOG = LoggerFactory.getLogger(LSNVectorLoggingManager.class);
    @Nonnull
    protected String loggingPath;
    @Nonnull protected LoggingOptions loggingOptions;
    public ConcurrentHashMap<Integer, LVLogRecord> threadToLVLogRecord = new ConcurrentHashMap<>();
    protected int parallelNum;
    protected Map<String, BaseTable> tables;
    public LSNVectorLoggingManager(Map<String, BaseTable> tables, Configuration configuration) {
        this.tables = tables;
        loggingPath = configuration.getString("rootFilePath") + OsUtils.OS_wrapper("logging");
        parallelNum = configuration.getInt("parallelNum");
        loggingOptions = new LoggingOptions(parallelNum, configuration.getString("compressionAlg"));
        for (int i = 0; i < parallelNum; i ++) {
            this.threadToLVLogRecord.put(i, new LVLogRecord(i));
        }
    }
    public LSNVectorLoggingResources syncPrepareResource(int partitionId) {
        return new LSNVectorLoggingResources(partitionId, this.threadToLVLogRecord.get(partitionId));
    }
    @Override
    public void addLogRecord(LoggingEntry logRecord) {
        LVCLog lvcLog = (LVCLog) logRecord;
        LVLogRecord lvLogRecord = threadToLVLogRecord.get(lvcLog.threadId);
        TableRecord tableRecord = this.tables.get(lvcLog.tableName).SelectKeyRecord(lvcLog.key);
        TableRecord[] conditions = new TableRecord[lvcLog.condition.length];
        for (int i = 0; i < lvcLog.condition.length; i ++) {
            conditions[i] = this.tables.get(lvcLog.tableName).SelectKeyRecord(lvcLog.condition[i]);
        }
        lvLogRecord.addLog(lvcLog, tableRecord, parallelNum, conditions);
    }

    @Override
    public void commitLog(long groupId, int partitionId, FTManager ftManager) throws IOException {
        NIOLSNVectorStreamFactory lsnVectorStreamFactory = new NIOLSNVectorStreamFactory(loggingPath);
        LSNVectorLoggingResources resources = syncPrepareResource(partitionId);
        AsynchronousFileChannel afc = lsnVectorStreamFactory.createLoggingStream();
        Attachment attachment = new Attachment(lsnVectorStreamFactory.getPath(), groupId, partitionId,afc, ftManager);
        ByteBuffer dataBuffer = resources.createWriteBuffer(loggingOptions);
        afc.write(dataBuffer, 0, attachment, new LoggingHandler());
    }

    @Override
    public void syncRetrieveLogs(RedoLogResult redoLogResult) throws IOException, ExecutionException, InterruptedException {

    }

    @Override
    public void registerTable(RecordSchema recordSchema, String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean inspectAbortView(long bid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object inspectDependencyView(long groupId, String table, String from, String to, long bid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public HashMap<String, List<Integer>> inspectTaskPlacing(long groupId, int threadId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public HistoryViews getHistoryViews() {
        throw new UnsupportedOperationException();
    }
}
