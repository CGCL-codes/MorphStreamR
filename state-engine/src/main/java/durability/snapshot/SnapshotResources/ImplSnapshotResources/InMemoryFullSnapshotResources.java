package durability.snapshot.SnapshotResources.ImplSnapshotResources;

import durability.snapshot.SnapshotOptions;
import durability.snapshot.SnapshotResources.SnapshotResources;
import durability.snapshot.SnapshotResources.StateMetaInfoSnapshot;
import durability.snapshot.SnapshotStrategy.ImplSnapshotStrategy.InMemorySnapshotStrategy;
import storage.TableRecord;
import storage.table.BaseTable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryFullSnapshotResources implements SnapshotResources {
    private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = new ArrayList<>();
    private final HashMap<String, HashMap<String, TableRecord>> snapshotResource = new HashMap<>();
    private long snapshotId;
    private int partitionId;

    public InMemoryFullSnapshotResources(long snapshotId, int partitionId, Map<String, InMemorySnapshotStrategy.InMemoryKvStateInfo> kvStateInformation, Map<String, BaseTable> tables) {
        this.snapshotId = snapshotId;
        this.partitionId = partitionId;
        createStateMetaInfoSnapshot(kvStateInformation);
        createSnapshotResources(tables);
    }

    private void createStateMetaInfoSnapshot(Map<String, InMemorySnapshotStrategy.InMemoryKvStateInfo> kvStateInformation) {
        for (InMemorySnapshotStrategy.InMemoryKvStateInfo info : kvStateInformation.values()) {
            this.stateMetaInfoSnapshots.add(new StateMetaInfoSnapshot(info.recordSchema, info.tableName, this.partitionId));
        }
    }
    private void createSnapshotResources(Map<String, BaseTable> tables) {
        for (Map.Entry<String, BaseTable> table:tables.entrySet()) {
            snapshotResource.put(table.getKey(), table.getValue().getTableIndexByPartitionId(this.partitionId));
        }
    }

    public ByteBuffer createWriteBuffer(SnapshotOptions snapshotOptions) {
        return null;
    }

    @Override
    public void release() {

    }
}
