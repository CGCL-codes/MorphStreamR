package durability.logging.LoggingResource.ImplLoggingResources;

import common.io.ByteIO.DataOutputView;
import durability.logging.LoggingEntry.LogRecord;
import durability.logging.LoggingStrategy.ImplLoggingManager.WALManager;
import durability.logging.LoggingResource.WalMetaInfoSnapshot;
import durability.logging.LoggingResource.LoggingResources;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class PartitionWalResources implements LoggingResources {
    private final List<WalMetaInfoSnapshot>  metaInfoSnapshots = new ArrayList<>();
    private final HashMap<String, ConcurrentSkipListSet<LogRecord>> logResources = new HashMap<>();
    private final long groupId;
    private final int partitionId;

    public PartitionWalResources(long groupId, int partitionId, Map<String, Map<Integer, ConcurrentSkipListSet<LogRecord>>> pendingEntries, ConcurrentHashMap<String, WALManager.WriteAheadLogTableInfo> metaInformation) {
        this.groupId = groupId;
        this.partitionId = partitionId;
        createLogMetaInfoSnapshots(metaInformation);
        createLogResources(pendingEntries);
    }
    private void createLogMetaInfoSnapshots(ConcurrentHashMap<String, WALManager.WriteAheadLogTableInfo> metaInformation) {
        for (WALManager.WriteAheadLogTableInfo info : metaInformation.values()) {
            this.metaInfoSnapshots.add(new WalMetaInfoSnapshot(info.tableName, this.partitionId));
        }
    }

    private void createLogResources(Map<String, Map<Integer, ConcurrentSkipListSet<LogRecord>>> pendingEntries) {
        for (Map.Entry<String, Map<Integer, ConcurrentSkipListSet<LogRecord>>> entry: pendingEntries.entrySet()) {
            this.logResources.put(entry.getKey(), entry.getValue().get(this.partitionId));
        }
    }
    public ByteBuffer createWriteBuffer() throws IOException {
        return null;
    }

    private void writeLogMetaData(DataOutputView dataOutputView) {

    }

    private void writeLogRecord(DataOutputView dataOutputView) {

    }
}
