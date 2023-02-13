package durability.ftmanager.ImplFTManager;

import common.collections.Configuration;
import durability.ftmanager.FTManager;
import durability.struct.Result.persistResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.FaultToleranceConstants;

import java.io.IOException;

public class PathManager extends FTManager {
    private static final Logger LOG = LoggerFactory.getLogger(PathManager.class);
    @Override
    public void initialize(Configuration config) throws IOException {

    }

    @Override
    public boolean spoutRegister(long snapshotId, String path) {
        return false;
    }

    @Override
    public persistResult spoutAskRecovery(int taskId, long snapshotOffset) {
        return null;
    }

    @Override
    public long sinkAskLastTask(int taskId) {
        return 0;
    }

    @Override
    public boolean sinkRegister(long snapshot) {
        return false;
    }

    @Override
    public boolean boltRegister(int partitionId, FaultToleranceConstants.FaultToleranceStatus status, persistResult Result) {
        return false;
    }

    @Override
    public void Listener() throws IOException {

    }
}
