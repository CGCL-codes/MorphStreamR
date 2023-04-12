package durability.logging.LoggingStrategy.ImplLoggingManager;

import common.collections.Configuration;
import common.collections.OsUtils;
import durability.ftmanager.FTManager;
import durability.logging.LoggingResource.ImplLoggingResources.DependencyLoggingResources;
import durability.logging.LoggingResult.Attachment;
import durability.logging.LoggingResult.LoggingHandler;
import durability.logging.LoggingStrategy.LoggingManager;
import durability.logging.LoggingStream.ImplLoggingStreamFactory.NIODependencyStreamFactory;
import durability.recovery.RedoLogResult;
import durability.recovery.histroyviews.HistoryViews;
import durability.snapshot.LoggingOptions;
import durability.struct.Logging.DependencyLog;
import durability.struct.Logging.LoggingEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.table.RecordSchema;
import utils.lib.ConcurrentHashMap;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutionException;

public class DependencyLoggingManager implements LoggingManager {
    private static final Logger LOG = LoggerFactory.getLogger(DependencyLoggingManager.class);
    @Nonnull
    protected String loggingPath;
    @Nonnull protected LoggingOptions loggingOptions;
    protected int parallelNum;
    protected final int num_items;
    protected final int delta;
    protected ConcurrentHashMap<Integer, Vector<DependencyLog>> threadToDependencyLog = new ConcurrentHashMap<>();
    public DependencyLoggingManager(Configuration configuration) {
        loggingPath = configuration.getString("rootFilePath") + OsUtils.OS_wrapper("logging");
        parallelNum = configuration.getInt("parallelNum");
        loggingOptions = new LoggingOptions(parallelNum, configuration.getString("compressionAlg"));
        num_items = configuration.getInt("NUM_ITEMS");
        delta = num_items / parallelNum;
        for (int i = 0; i < parallelNum; i ++) {
            this.threadToDependencyLog.put(i, new Vector<>());
        }
    }
    public DependencyLoggingResources syncPrepareResource(int partitionId) throws IOException {
        return new DependencyLoggingResources(partitionId, this.threadToDependencyLog.get(partitionId));
    }
    @Override
    public void addLogRecord(LoggingEntry logRecord) {
        DependencyLog dependencyLog = (DependencyLog) logRecord;
        if (!this.threadToDependencyLog.get(getPartitionId(dependencyLog.key)).contains(logRecord))
            this.threadToDependencyLog.get(getPartitionId(dependencyLog.key)).add(dependencyLog);
    }


    @Override
    public void commitLog(long groupId, int partitionId, FTManager ftManager) throws IOException {
        NIODependencyStreamFactory dependencyStreamFactory = new NIODependencyStreamFactory(loggingPath);
        DependencyLoggingResources dependencyLoggingResources = syncPrepareResource(partitionId);
        AsynchronousFileChannel afc = dependencyStreamFactory.createLoggingStream();
        Attachment attachment = new Attachment(dependencyStreamFactory.getPath(), groupId, partitionId, afc, ftManager);
        ByteBuffer dataBuffer = dependencyLoggingResources.createWriteBuffer(loggingOptions);
        afc.write(dataBuffer, 0, attachment, new LoggingHandler());
    }

    @Override
    public void syncRetrieveLogs(RedoLogResult redoLogResult) throws IOException, ExecutionException, InterruptedException {

    }

    @Override
    public void registerTable(RecordSchema recordSchema, String tableName) {
        throw new UnsupportedOperationException("DependencyLoggingManager does not support registerTable");
    }

    @Override
    public boolean inspectAbortView(long bid) {
        throw new UnsupportedOperationException("DependencyLoggingManager does not support inspectAbortView");
    }

    @Override
    public Object inspectDependencyView(long groupId, String table, String from, String to, long bid) {
        throw new UnsupportedOperationException("DependencyLoggingManager does not support inspectDependencyView");
    }

    @Override
    public HashMap<String, List<Integer>> inspectTaskPlacing(long groupId, int threadId) {
       throw new UnsupportedOperationException("DependencyLoggingManager does not support inspectTaskPlacing");
    }

    @Override
    public HistoryViews getHistoryViews() {
        throw new UnsupportedOperationException("DependencyLoggingManager does not support getHistoryViews");
    }
    public int getPartitionId(String primary_key) {
        int key = Integer.parseInt(primary_key);
        return key / delta;
    }
}
