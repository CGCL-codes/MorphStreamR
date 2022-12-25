package durability.snapshot.SnapshotResources;

import storage.table.RecordSchema;

import java.io.Serializable;

public class StateMetaInfoSnapshot implements Serializable {
    public final RecordSchema recordSchema;
    public final String tableName;
    public final int partitionId;

    public StateMetaInfoSnapshot(RecordSchema recordSchema, String tableName, int partitionId) {
        this.recordSchema = recordSchema;
        this.tableName = tableName;
        this.partitionId = partitionId;
    }
}
