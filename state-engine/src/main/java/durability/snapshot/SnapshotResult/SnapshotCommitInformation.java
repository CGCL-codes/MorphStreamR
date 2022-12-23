package durability.snapshot.SnapshotResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

public class SnapshotCommitInformation {
    public final long snapshotId;
    public final List<SnapshotResult> snapshotResults = new Vector<>();

    public SnapshotCommitInformation(long snapshotId) {
        this.snapshotId = snapshotId;
    }
}


