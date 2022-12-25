package durability.wal;

import common.collections.Configuration;
import common.collections.OsUtils;
import durability.wal.WalEntry.LogRecord;
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
    @Nonnull protected Map<String, Map<Integer, ConcurrentSkipListSet<LogRecord>>> pendingEntries;
    @Nonnull protected String walPath;
    @Nonnull protected int parallelNum;
    @Nonnull protected ConcurrentHashMap<String, WriteAheadLogTableInfo> metaInformation;
    public WALManager(Configuration configuration) {
        parallelNum = configuration.getInt("parallelNum");
        walPath = configuration.getString("rootFilePath") + OsUtils.OS_wrapper("wal");
        metaInformation = new ConcurrentHashMap<>();
        pendingEntries = new ConcurrentHashMap<>();
    }

    public static class WriteAheadLogTableInfo implements Serializable {
        public final String tableName;
        public WriteAheadLogTableInfo(String tableName) {
            this.tableName = tableName;
        }
    }

    public void registerTableToWAL(String tableName) {
        metaInformation.put(tableName, new WriteAheadLogTableInfo(tableName));
        ConcurrentHashMap<Integer, ConcurrentSkipListSet<LogRecord>> logs = new ConcurrentHashMap<>();
        for (int i = 0; i < parallelNum; i++) {
            logs.put(i, new ConcurrentSkipListSet<>());
        }
        pendingEntries.put(tableName, logs);
    }

}
