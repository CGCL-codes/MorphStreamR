package durability.logging.LoggingStrategy;

import durability.ftmanager.FTManager;
import durability.recovery.RedoLogResult;
import durability.struct.Logging.LoggingEntry;
import storage.table.RecordSchema;

import java.io.IOException;

public interface LoggingManager {
    void registerTable(RecordSchema recordSchema, String tableName);
    void addLogRecord(LoggingEntry logRecord);
    void commitLog(long groupId, int partitionId, FTManager ftManager) throws IOException;
    void syncRedoWriteAheadLog(RedoLogResult redoLogResult) throws IOException;
}
