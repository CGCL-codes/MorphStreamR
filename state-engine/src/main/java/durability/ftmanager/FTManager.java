package durability.ftmanager;

import common.collections.Configuration;
import durability.struct.Result.persistResult;
import utils.FaultToleranceConstants;

import java.io.IOException;

public abstract class FTManager extends Thread{

    public boolean running = true;
    public abstract void initialize(Configuration config) throws IOException;
    /**
     * @param snapshotId
     * @param message
     * @param path input store path
     */
    public abstract boolean spoutRegister(long snapshotId, String message, String path);
    public abstract boolean sinkRegister(long snapshot);
    /**
     * @param partitionId
     * @param status
     * @param Result
     * */
    public abstract boolean boltRegister(int partitionId, FaultToleranceConstants.FaultToleranceStatus status, persistResult Result);
    public abstract void Listener() throws IOException;
}
