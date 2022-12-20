package durability.manager.ImplFTManager;

import durability.manager.FTManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.FaultToleranceConstants;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

public class CheckpointManager extends Thread implements FTManager {
    private final Logger LOG = LoggerFactory.getLogger(CheckpointManager.class);
    public boolean running = true;
    private ConcurrentHashMap<Integer, FaultToleranceConstants.FaultToleranceStatus> callCommit;
    private Path metaPath;
    @Override
    public void initialize() {

    }

    @Override
    public boolean spoutRegister(long snapshotId) {
        return false;
    }

    @Override
    public boolean sinkRegister(long snapshot) {
        return false;
    }

    @Override
    public boolean boltRegister(int executorId, FaultToleranceConstants.FaultToleranceStatus status) {
        return false;
    }

    @Override
    public void Listener() {

    }

    @Override
    public void run() {
        super.run();
    }
}
