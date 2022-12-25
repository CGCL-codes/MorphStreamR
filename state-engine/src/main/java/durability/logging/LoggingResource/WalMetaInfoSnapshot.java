package durability.logging.LoggingResource;

import java.io.Serializable;

public class WalMetaInfoSnapshot implements Serializable {
    public final String tableName;
    public final int partitionId;

    public WalMetaInfoSnapshot(String tableName, int partitionId) {
        this.tableName = tableName;
        this.partitionId = partitionId;
    }
}
