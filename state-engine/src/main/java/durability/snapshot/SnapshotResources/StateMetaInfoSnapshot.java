package durability.snapshot.SnapshotResources;

import storage.table.RecordSchema;

import java.io.Serializable;

public class StateMetaInfoSnapshot implements Serializable {
    private final RecordSchema recordSchema;
    private final String tableName;
    private final int partitionId;

    public StateMetaInfoSnapshot(RecordSchema recordSchema, String tableName, int partitionId) {
        this.recordSchema = recordSchema;
        this.tableName = tableName;
        this.partitionId = partitionId;
    }
}
