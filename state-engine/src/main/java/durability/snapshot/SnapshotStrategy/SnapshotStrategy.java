package durability.snapshot.SnapshotStrategy;

import durability.snapshot.SnapshotResources.SnapshotResources;

import java.util.List;

public interface SnapshotStrategy<SR extends SnapshotResources> {

    /**
     * Performs the synchronous part of the snapshot. It returns resources which can be later
     * on used in the asynchronous
     * @param checkpointId the ID of the shapshot
     * @return Resources needed to finish the snapshot
     * @throws Exception
     */
    SR syncPrepareResources(long checkpointId) throws Exception;
    List<SR> syncPrepareResources(long checkpointId, int parallelNum) throws Exception;

    String getDescription();
}
