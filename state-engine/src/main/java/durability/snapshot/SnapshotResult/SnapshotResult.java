package durability.snapshot.SnapshotResult;

import java.nio.file.Path;

/**
 * The snapshot result
 * Snapshot Path
 * Offset
 * */
public class SnapshotResult {
    public final Path path;
    public final long snapshotId;
    public final int partitionId;


    public SnapshotResult(long snapshotId, int partitionId, Path path) {
        this.snapshotId = snapshotId;
        this.partitionId = partitionId;
        this.path = path;
    }
}
