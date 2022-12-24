package durability.snapshot.SnapshotResult;

import java.io.Serializable;
import java.util.List;
import java.util.Vector;

public class SnapshotCommitInformation implements Serializable {
    public final long snapshotId;
    public final List<SnapshotResult> snapshotResults = new Vector<>();

    public SnapshotCommitInformation(long snapshotId) {
        this.snapshotId = snapshotId;
    }
}


