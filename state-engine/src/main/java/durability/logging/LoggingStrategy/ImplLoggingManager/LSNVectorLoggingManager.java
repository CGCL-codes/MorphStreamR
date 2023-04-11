package durability.logging.LoggingStrategy.ImplLoggingManager;

import common.collections.Configuration;
import common.collections.OsUtils;
import durability.ftmanager.FTManager;
import durability.logging.LoggingEntry.LVLogRecord;
import durability.logging.LoggingStrategy.LoggingManager;
import durability.recovery.RedoLogResult;
import durability.recovery.histroyviews.HistoryViews;
import durability.snapshot.LoggingOptions;
import durability.struct.Logging.LoggingEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.table.RecordSchema;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import utils.lib.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class LSNVectorLoggingManager implements LoggingManager {
    private static final Logger LOG = LoggerFactory.getLogger(LSNVectorLoggingManager.class);
    @Nonnull
    protected String loggingPath;
    @Nonnull protected LoggingOptions loggingOptions;
    public ConcurrentHashMap<Integer, LVLogRecord> threadToLVLogRecord = new ConcurrentHashMap<>();
    protected int parallelNum;
    public LSNVectorLoggingManager(Configuration configuration) {
        loggingPath = configuration.getString("rootFilePath") + OsUtils.OS_wrapper("logging");
        parallelNum = configuration.getInt("parallelNum");
        loggingOptions = new LoggingOptions(parallelNum, configuration.getString("compressionAlg"));
        for (int i = 0; i < parallelNum; i ++) {
            this.threadToLVLogRecord.put(i, new LVLogRecord(i));
        }
    }

    @Override
    public void commitLog(long groupId, int partitionId, FTManager ftManager) throws IOException {

    }

    @Override
    public void syncRetrieveLogs(RedoLogResult redoLogResult) throws IOException, ExecutionException, InterruptedException {

    }

    @Override
    public void registerTable(RecordSchema recordSchema, String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addLogRecord(LoggingEntry logRecord) {
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
