package durability.wal;

import common.collections.Configuration;
import common.collections.OsUtils;
import durability.manager.FTManager;
import durability.wal.WalEntry.LogRecord;
import durability.wal.WalEntry.Update;
import durability.wal.WalResource.ImplWalResources.PartitionWalResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class WALManager {
    private static final Logger LOG = LoggerFactory.getLogger(WALManager.class);
    //<TableName, <PartitionId, LogRecords>>
    @Nonnull protected Map<String, Map<Integer, ConcurrentSkipListSet<Update>>> pendingEntries;
    @Nonnull protected String walPath;
    @Nonnull protected int parallelNum;
    @Nonnull protected ConcurrentHashMap<String, WriteAheadLogTableInfo> metaInformation;
    public WALManager(Configuration configuration) {
        parallelNum = configuration.getInt("parallelNum");
        walPath = configuration.getString("rootFilePath") + OsUtils.OS_wrapper("wal");
        metaInformation = new ConcurrentHashMap<>();
        pendingEntries = new ConcurrentHashMap<>();
    }
    public void registerTableToWAL(String tableName) {
        metaInformation.put(tableName, new WriteAheadLogTableInfo(tableName));
        ConcurrentHashMap<Integer, ConcurrentSkipListSet<Update>> logs = new ConcurrentHashMap<>();
        for (int i = 0; i < parallelNum; i++) {
            logs.put(i, new ConcurrentSkipListSet<>());
        }
        pendingEntries.put(tableName, logs);
    }

    public void addLogRecord(LogRecord logRecord) {
        this.pendingEntries.get(logRecord.tableName).get(logRecord.partitionId).add(logRecord.update);
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

}
