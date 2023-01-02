package durability.ftmanager.ImplFTManager;

import common.collections.Configuration;
import common.collections.OsUtils;
import common.io.LocalFS.FileSystem;
import common.io.LocalFS.LocalDataOutputStream;
import common.tools.Serialize;
import durability.ftmanager.FTManager;
import durability.recovery.RecoveryHelperProvider;
import durability.snapshot.SnapshotResult.SnapshotCommitInformation;
import durability.snapshot.SnapshotResult.SnapshotResult;
import durability.struct.Result.persistResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static utils.FaultToleranceConstants.*;

/**
 * Input-Store&State Checkpoint (ISC)
 */
public class CheckpointManager extends FTManager {
    private final Logger LOG = LoggerFactory.getLogger(CheckpointManager.class);
    private int parallelNum;
    private ConcurrentHashMap<Long, List<FaultToleranceStatus>> callCommit = new ConcurrentHashMap<>();
    //<snapshotId, SnapshotCommitInformation>
    private ConcurrentHashMap<Long, SnapshotCommitInformation> registerSnapshot = new ConcurrentHashMap<>();
    private String metaPath;
    private String basePath;
    private String inputStoreRootPath;
    private Queue<Long> uncommittedId = new ConcurrentLinkedQueue<>();
    private long pendingSnapshotId;
    /** Used during recovery */
    private boolean isRecovery;
    private SnapshotCommitInformation latestSnapshotCommitInformation;
    @Override
    public void initialize(Configuration config) throws IOException {
        this.parallelNum = config.getInt("parallelNum");
        basePath = config.getString("rootFilePath") + OsUtils.OS_wrapper("snapshot");
        metaPath = config.getString("rootFilePath") + OsUtils.OS_wrapper("snapshot") + OsUtils.OS_wrapper("metaData.log");
        inputStoreRootPath = config.getString("rootFilePath") + OsUtils.OS_wrapper("inputStore");
        isRecovery = config.getBoolean("isRecovery");
        File file = new File(this.basePath);
        if (!file.exists()) {
            file.mkdirs();
        }
        if (isRecovery) {
            latestSnapshotCommitInformation = RecoveryHelperProvider.getLatestCommitSnapshotCommitInformation(new File(metaPath));
        } else {
            SnapshotCommitInformation snapshotCommitInformation = new SnapshotCommitInformation(0L, config.getString("rootFilePath") + OsUtils.OS_wrapper("inputStore") + OsUtils.OS_wrapper("inputStore"));
            byte[] result = Serialize.serializeObject(snapshotCommitInformation);
            LocalDataOutputStream localDataOutputStream = new LocalDataOutputStream(new File(this.metaPath));
            DataOutputStream dataOutputStream = new DataOutputStream(localDataOutputStream);
            int length = result.length;
            dataOutputStream.writeInt(length);
            dataOutputStream.write(result);
            dataOutputStream.close();
        }
        this.setName("CheckpointManager");
        LOG.info("CheckpointManager initialize successfully");
    }

    @Override
    public boolean spoutRegister(long snapshotId, String path) {
        if (this.registerSnapshot.containsKey(snapshotId)) {
            //TODO: if these are too many uncommitted snapshot, notify the spout not to register
            LOG.info("SnapshotID has been registered already");
            return false;
        } else {
            this.registerSnapshot.put(snapshotId, new SnapshotCommitInformation(snapshotId, path));
            callCommit.put(snapshotId, initCallCommit());
            this.uncommittedId.add(snapshotId);
            LOG.info("Register snapshot with offset: " + snapshotId + "; pending snapshot: " + uncommittedId.size());
            return true;
        }
    }

    @Override
    public persistResult spoutAskRecovery(int taskId, long snapshotOffset) {
        return latestSnapshotCommitInformation.snapshotResults.get(taskId);
    }

    @Override
    public boolean sinkRegister(long snapshot) {
        return false;
    }

    @Override
    public boolean boltRegister(int partitionId, FaultToleranceStatus status, persistResult result) {
        SnapshotResult snapshotResult = (SnapshotResult) result;
        this.registerSnapshot.get(snapshotResult.snapshotId).snapshotResults.put(snapshotResult.partitionId, snapshotResult);
        this.callCommit.get(snapshotResult.snapshotId).set(partitionId, FaultToleranceStatus.Snapshot);
        return true;
    }

    @Override
    public void Listener() throws IOException {
        while (running) {
            if (all_register()) {
                if (callCommit.get(pendingSnapshotId).contains(FaultToleranceStatus.Snapshot)) {
                    LOG.info("CheckpointManager received all register and commit snapshot");
                    snapshotComplete(pendingSnapshotId);
                    if (uncommittedId.size() != 0) {
                        this.pendingSnapshotId = uncommittedId.poll();
                    } else {
                        this.pendingSnapshotId = 0;
                    }
                    LOG.info("Pending snapshot: " + uncommittedId.size());
                }
            }
        }
    }

    @Override
    public void run() {
        LOG.info("CheckpointManager starts!");
        try {
            Listener();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (isRecovery) {
                File file = new File(this.basePath);
                FileSystem.deleteFile(file);
                file = new File(this.inputStoreRootPath);
                FileSystem.deleteFile(file);
            }
            LOG.info("CheckpointManager stops");
        }
    }

    private boolean all_register() {
        if (pendingSnapshotId == 0) {
            if (uncommittedId.size() != 0) {
                pendingSnapshotId = uncommittedId.poll();
                return !this.callCommit.get(pendingSnapshotId).contains(FaultToleranceStatus.NULL);
            } else {
                return false;
            }
        } else {
            return !this.callCommit.get(pendingSnapshotId).contains(FaultToleranceStatus.NULL);
        }
    }

    private List<FaultToleranceStatus> initCallCommit() {
        List<FaultToleranceStatus> statuses = new Vector<>();
        for (int i = 0; i < parallelNum; i++) {
            statuses.add(FaultToleranceStatus.NULL);
        }
        return statuses;
    }

    private void snapshotComplete(long snapshotId) throws IOException {
        SnapshotCommitInformation snapshotCommitInformation = this.registerSnapshot.get(snapshotId);
        LocalDataOutputStream localDataOutputStream = new LocalDataOutputStream(new File(this.metaPath));
        DataOutputStream dataOutputStream = new DataOutputStream(localDataOutputStream);
        byte[] result = Serialize.serializeObject(snapshotCommitInformation);
        int length = result.length;
        dataOutputStream.writeInt(length);
        dataOutputStream.write(result);
        dataOutputStream.close();
        this.registerSnapshot.remove(snapshotId);
        LOG.info("CheckpointManager commit the snapshot to the current.log");
    }
}
