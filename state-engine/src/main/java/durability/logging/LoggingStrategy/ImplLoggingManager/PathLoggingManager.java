package durability.logging.LoggingStrategy.ImplLoggingManager;

import durability.ftmanager.FTManager;
import durability.logging.LoggingEntry.LogRecord;
import durability.logging.LoggingStrategy.LoggingManager;
import durability.recovery.RedoLogResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.table.RecordSchema;

import java.io.IOException;

public class PathLoggingManager implements LoggingManager {
    //TODO: Tracking exploration for different schedulers
    //TODO: Tracking transactions which is aborted
    //TODO: How to track UDF?
    private static final Logger LOG = LoggerFactory.getLogger(WALManager.class);

    @Override
    public void registerTable(RecordSchema recordSchema, String tableName) {

    }

    @Override
    public void addLogRecord(LogRecord logRecord) {

    }

    @Override
    public void commitLog(long groupId, int partitionId, FTManager ftManager) throws IOException {

    }

    @Override
    public void syncRedoWriteAheadLog(RedoLogResult redoLogResult) throws IOException {

    }

    public static Logger getLOG() {
        return LOG;
    }
}
