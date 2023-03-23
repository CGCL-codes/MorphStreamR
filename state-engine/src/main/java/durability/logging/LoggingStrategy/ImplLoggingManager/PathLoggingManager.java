package durability.logging.LoggingStrategy.ImplLoggingManager;

import common.collections.Configuration;
import common.collections.OsUtils;
import durability.ftmanager.FTManager;
import durability.logging.LoggingEntry.PathRecord;
import durability.logging.LoggingResource.ImplLoggingResources.DependencyMaintainResources;
import durability.logging.LoggingResult.Attachment;
import durability.logging.LoggingResult.LoggingHandler;
import durability.logging.LoggingStrategy.LoggingManager;
import durability.logging.LoggingStream.ImplLoggingStreamFactory.NIOPathStreamFactory;
import durability.recovery.RedoLogResult;
import durability.snapshot.LoggingOptions;
import durability.struct.Logging.LoggingEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.table.RecordSchema;
import utils.lib.ConcurrentHashMap;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;

public class PathLoggingManager implements LoggingManager {
    private static final Logger LOG = LoggerFactory.getLogger(PathLoggingManager.class);
    @Nonnull protected String loggingPath;
    @Nonnull protected LoggingOptions loggingOptions;
    protected int parallelNum;
    public ConcurrentHashMap<Integer, PathRecord> threadToPathRecord = new ConcurrentHashMap<>();
    public PathLoggingManager(Configuration configuration) {
        loggingPath = configuration.getString("rootFilePath") + OsUtils.OS_wrapper("logging");
        parallelNum = configuration.getInt("parallelNum");
        loggingOptions = new LoggingOptions(parallelNum, configuration.getString("compressionAlg"));
        for (int i = 0; i < parallelNum; i ++) {
            this.threadToPathRecord.put(i, new PathRecord());
        }
    }
    public DependencyMaintainResources syncPrepareResource(int partitionId) {
        return new DependencyMaintainResources(partitionId, this.threadToPathRecord.get(partitionId));
    }

    @Override
    public void registerTable(RecordSchema recordSchema, String tableName) {
        throw new UnsupportedOperationException("Not supported yet");
    }

    @Override
    public void addLogRecord(LoggingEntry logRecord) {
        throw new UnsupportedOperationException("Not supported yet");
    }

    @Override
    public void commitLog(long groupId, int partitionId, FTManager ftManager) throws IOException {
        NIOPathStreamFactory nioPathStreamFactory = new NIOPathStreamFactory(this.loggingPath);
        DependencyMaintainResources dependencyMaintainResources = syncPrepareResource(partitionId);
        AsynchronousFileChannel afc = nioPathStreamFactory.createLoggingStream();
        Attachment attachment = new Attachment(nioPathStreamFactory.getPath(), groupId, partitionId, afc, ftManager);
        ByteBuffer dataBuffer = dependencyMaintainResources.createWriteBuffer(loggingOptions);
        afc.write(dataBuffer, 0, attachment, new LoggingHandler());
    }

    @Override
    public void syncRedoWriteAheadLog(RedoLogResult redoLogResult) throws IOException {
        throw new UnsupportedOperationException("Not supported yet");
    }

    public static Logger getLOG() {
        return LOG;
    }
}
