package durability.ftmanager.ImplFTManager;

import common.collections.Configuration;
import common.collections.OsUtils;
import common.io.LocalFS.FileSystem;
import common.io.LocalFS.LocalDataOutputStream;
import common.tools.Serialize;
import durability.ftmanager.FTManager;
import durability.logging.LoggingResult.LoggingCommitInformation;
import durability.logging.LoggingResult.LoggingResult;
import durability.struct.Result.persistResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.FaultToleranceConstants.FaultToleranceStatus;
import utils.lib.ConcurrentHashMap;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Write-Ahead-Log&State Checkpoint (WSC)
 * */
public class WalManager extends FTManager {
    private final Logger LOG = LoggerFactory.getLogger(FTManager.class);
    private int parallelNum;
    private ConcurrentHashMap<Long, List<FaultToleranceStatus>> callCommit = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, LoggingCommitInformation> registerCommit = new ConcurrentHashMap<>();
    private String walMetaPath;
    private String walPath;

    private Queue<Long> uncommittedId = new ConcurrentLinkedQueue<>();

    private long pendingId;
    @Override
    public void initialize(Configuration config) {
        this.parallelNum = config.getInt("parallelNum");
        walPath = config.getString("rootFilePath") + OsUtils.OS_wrapper("logging");
        walMetaPath = config.getString("rootFilePath") + OsUtils.OS_wrapper("logging") + OsUtils.OS_wrapper("metaData.log");
        File walFile = new File(walPath);
        if (!walFile.exists()) {
            walFile.mkdirs();
        }
        LOG.info("WalManager initialize successfully");
        this.setName("WalManager");
    }

    @Override
    public boolean spoutRegister(long groupId, String message) {
        if (this.registerCommit.containsKey(groupId)) {
            //TODO: if these are too many uncommitted group, notify the spout not to register
            LOG.info("groupID has been registered already");
            return false;
        } else {
            this.registerCommit.put(groupId, new LoggingCommitInformation(groupId));
            callCommit.put(groupId, initCallCommit());
            this.uncommittedId.add(groupId);
            LOG.info("Register group with offset: " + groupId + "; pending group: " + uncommittedId.size());
            return true;
        }
    }

    @Override
    public boolean sinkRegister(long snapshot) {
        return false;
    }

    @Override
    public boolean boltRegister(int partitionId, FaultToleranceStatus status, persistResult result) {
        LoggingResult loggingResult = (LoggingResult) result;
        this.registerCommit.get(loggingResult.groupId).loggingResults.add(loggingResult);
        this.callCommit.get(loggingResult.groupId).set(partitionId, FaultToleranceStatus.Persist);
        return true;
    }

    @Override
    public void Listener() throws IOException {
        while (running) {
            if (all_register_commit()) {
                if (callCommit.get(pendingId).contains(FaultToleranceStatus.Persist)) {
                    LOG.info("WalManager received all register and commit log");
                    logComplete(pendingId);
                    if (uncommittedId.size() != 0) {
                        this.pendingId = uncommittedId.poll();
                    } else {
                        this.pendingId = 0;
                    }
                    LOG.info("Pending commit: " + uncommittedId.size());
                }
            }
        }
    }
    public void run() {
        LOG.info("WalManager starts!");
        try {
            Listener();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            File file = new File(this.walPath);
            FileSystem.deleteFile(file);
            LOG.info("WalManager stops");
        }
    }


    private void logComplete(long pendingId) throws IOException {
        LoggingCommitInformation loggingCommitInformation = this.registerCommit.get(pendingId);
        LocalDataOutputStream localDataOutputStream = new LocalDataOutputStream(new File(this.walMetaPath));
        DataOutputStream dataOutputStream = new DataOutputStream(localDataOutputStream);
        byte[] result = Serialize.serializeObject(loggingCommitInformation);
        int length = result.length;
        dataOutputStream.writeInt(length);
        dataOutputStream.write(result);
        dataOutputStream.close();
        this.registerCommit.remove(pendingId);
        LOG.info("WalManager commit the wal to the current.log");
    }

    private List<FaultToleranceStatus> initCallCommit() {
        List<FaultToleranceStatus> statuses = new Vector<>();
        for (int i = 0; i < parallelNum; i++) {
            statuses.add(FaultToleranceStatus.NULL);
        }
        return statuses;
    }
    private boolean all_register_commit() {
        if (pendingId == 0) {
            if (uncommittedId.size() != 0) {
                pendingId = uncommittedId.poll();
                return !this.callCommit.get(pendingId).contains(FaultToleranceStatus.NULL);
            } else {
                return false;
            }
        } else {
            return !this.callCommit.get(pendingId).contains(FaultToleranceStatus.NULL);
        }
    }
}
