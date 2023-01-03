package durability.logging.LoggingResource;

import storage.table.RecordSchema;

import java.io.Serializable;

public class WalMetaInfoSnapshot implements Serializable {
    public final RecordSchema recordSchema;
    public final String tableName;
    public final int partitionId;
    public int logRecordNumber;

    public WalMetaInfoSnapshot(RecordSchema recordSchema, String tableName, int partitionId) {
        this.recordSchema = recordSchema;
        this.tableName = tableName;
        this.partitionId = partitionId;
    }
    public void setLogRecordNumber(int logRecordNumber) {
        this.logRecordNumber = logRecordNumber;
    }
}
