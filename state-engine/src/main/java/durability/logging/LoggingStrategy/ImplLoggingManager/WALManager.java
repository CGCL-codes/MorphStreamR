package durability.logging.LoggingStrategy.ImplLoggingManager;

import common.collections.Configuration;
import common.collections.OsUtils;
import durability.logging.LoggingStrategy.LoggingManager;
import durability.ftmanager.FTManager;
import durability.logging.LoggingEntry.LogRecord;
import durability.logging.LoggingResource.ImplLoggingResources.PartitionWalResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class WALManager implements LoggingManager {
    private static final Logger LOG = LoggerFactory.getLogger(WALManager.class);
    //<TableName, <PartitionId, LogRecords>>
    @Nonnull protected Map<String, Map<Integer, ConcurrentSkipListSet<LogRecord>>> pendingEntries;
    @Nonnull protected String walPath;
    @Nonnull protected int parallelNum;
    @Nonnull protected ConcurrentHashMap<String, WriteAheadLogTableInfo> metaInformation;
    @Nonnull protected final int num_items;
    @Nonnull protected final int delta;
    public WALManager(Configuration configuration) {
        parallelNum = configuration.getInt("parallelNum");
        walPath = configuration.getString("rootFilePath") + OsUtils.OS_wrapper("logging");
        num_items = configuration.getInt("NUM_ITEMS");
        delta = num_items / parallelNum;
        metaInformation = new ConcurrentHashMap<>();
        pendingEntries = new ConcurrentHashMap<>();
    }

    @Override
    public void registerTable(String tableName) {
        metaInformation.put(tableName, new WriteAheadLogTableInfo(tableName));
        ConcurrentHashMap<Integer, ConcurrentSkipListSet<LogRecord>> logs = new ConcurrentHashMap<>();
        for (int i = 0; i < parallelNum; i++) {
            logs.put(i, new ConcurrentSkipListSet<>());
        }
        pendingEntries.put(tableName, logs);
    }
    @Override
    public void addLogRecord(LogRecord logRecord) {
        this.pendingEntries.get(logRecord.tableName).get(getPartitionId(logRecord.key)).add(logRecord);
    }

    public PartitionWalResources syncPrepareResource(long groupId, int partitionId) {
        return new PartitionWalResources(groupId, partitionId, pendingEntries, metaInformation);
    }

    public void commitLog(long snapshotId, int partitionId, FTManager ftManager) {

    }

    public static class WriteAheadLogTableInfo implements Serializable {
        public final String tableName;
        public WriteAheadLogTableInfo(String tableName) {
            this.tableName = tableName;
        }
    }

    public int getPartitionId(String primary_key) {
        int key = Integer.parseInt(primary_key);
        return key / delta;
    }

}
