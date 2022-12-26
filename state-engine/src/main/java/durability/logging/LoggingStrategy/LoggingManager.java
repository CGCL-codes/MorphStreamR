package durability.logging.LoggingStrategy;

import durability.ftmanager.FTManager;
import durability.logging.LoggingEntry.LogRecord;

import java.io.IOException;

public interface LoggingManager {
    public void registerTable(String tableName);
    public void addLogRecord(LogRecord logRecord);
    public void commitLog(long groupId, int partitionId, FTManager ftManager) throws IOException;
}
