package durability.logging.LoggingStrategy;

import durability.ftmanager.FTManager;
import durability.logging.LoggingEntry.LogRecord;
import durability.recovery.RedoLogResult;
import storage.table.RecordSchema;

import java.io.IOException;

public interface LoggingManager {
    void registerTable(RecordSchema recordSchema, String tableName);
    void addLogRecord(LogRecord logRecord);
    void commitLog(long groupId, int partitionId, FTManager ftManager) throws IOException;
    void syncRedoWriteAheadLog(RedoLogResult redoLogResult) throws IOException;
}
