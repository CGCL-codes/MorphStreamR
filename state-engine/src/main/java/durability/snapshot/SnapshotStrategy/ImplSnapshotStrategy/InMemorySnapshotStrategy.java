package durability.snapshot.SnapshotStrategy.ImplSnapshotStrategy;

import durability.snapshot.SnapshotResources.ImplSnapshotResources.InMemoryFullSnapshotResources;
import durability.snapshot.SnapshotStrategy.SnapshotStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class InMemorySnapshotStrategy implements SnapshotStrategy<InMemoryFullSnapshotResources> {
    private static final Logger LOG = LoggerFactory.getLogger(InMemorySnapshotStrategy.class);
    private static final String DESCRIPTION = "Full snapshot of In-Memory Database";

    @Override
    public InMemoryFullSnapshotResources syncPrepareResources(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public List<InMemoryFullSnapshotResources> syncPrepareResources(long checkpointId, int parallelNum) throws Exception {
        return null;
    }

    @Override
    public String getDescription() {
        return DESCRIPTION;
    }
}
