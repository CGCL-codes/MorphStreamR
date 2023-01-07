package durability.logging.LoggingResult;

import durability.struct.Result.persistResult;

import java.io.Serializable;

public class LoggingResult implements Serializable, persistResult {
    public final String path;
    public final long groupId;
    public final int partitionId;
    public transient int size;// in MB


    public LoggingResult(long groupId, int partitionId, String path) {
        this.groupId = groupId;
        this.partitionId = partitionId;
        this.path = path;
    }
}
