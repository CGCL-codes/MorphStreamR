package durability.logging.LoggingStrategy;

import durability.ftmanager.FTManager;
import durability.logging.LoggingEntry.LogRecord;

import java.io.IOException;

public interface LoggingManager {
    void registerTable(String tableName);
    void addLogRecord(LogRecord logRecord);
    void commitLog(long groupId, int partitionId, FTManager ftManager) throws IOException;
}
