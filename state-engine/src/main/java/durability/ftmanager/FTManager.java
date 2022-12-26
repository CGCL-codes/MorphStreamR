package durability.ftmanager;

import common.collections.Configuration;
import durability.snapshot.SnapshotResult.SnapshotResult;
import durability.struct.Result.persistResult;
import utils.FaultToleranceConstants;

import java.io.IOException;

public abstract class FTManager extends Thread{

    public boolean running = true;
    public abstract void initialize(Configuration config);
    public abstract boolean spoutRegister(long snapshotId, String message);
    public abstract boolean sinkRegister(long snapshot);
    public abstract boolean boltRegister(int partitionId, FaultToleranceConstants.FaultToleranceStatus status, persistResult Result);
    public abstract void Listener() throws IOException;
}
