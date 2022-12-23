package db;

import common.collections.Configuration;
import durability.manager.FTManager;
import storage.EventManager;
import storage.StorageManager;
import storage.TableRecord;

/**
 * original designer for CavaliaDatabase: Yingjun Wu.
 */
public class CavaliaDatabase extends Database {
    public CavaliaDatabase(Configuration configuration) {
        storageManager = new StorageManager(configuration);
        eventManager = new EventManager();
    }

    /**
     * @param table
     * @param record
     * @throws DatabaseException
     */
    @Override
    public void InsertRecord(String table, TableRecord record, int partition_id) throws DatabaseException {
        storageManager.InsertRecord(table, record, partition_id);
    }

    @Override
    public void parallelSnapshot(long snapshotId, int partitionId, FTManager ftManager) throws Exception {
        this.storageManager.asyncSnapshot(snapshotId, partitionId, ftManager);
    }
}
