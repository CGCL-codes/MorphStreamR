package durability.snapshot.SnapshotResult;

import utils.lib.ConcurrentHashMap;

import java.io.Serializable;
import java.util.List;
import java.util.Vector;

public class SnapshotCommitInformation implements Serializable {
    public final long snapshotId;
    public final ConcurrentHashMap<Integer, SnapshotResult> snapshotResults = new ConcurrentHashMap<>();
    public final String inputStorePath;

    public SnapshotCommitInformation(long snapshotId, String inputStorePath) {
        this.snapshotId = snapshotId;
        this.inputStorePath = inputStorePath;
    }
}


