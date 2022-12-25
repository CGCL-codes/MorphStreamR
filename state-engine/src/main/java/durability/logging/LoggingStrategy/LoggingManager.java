package durability.logging.LoggingStrategy;

import durability.logging.LoggingEntry.LogRecord;

public interface LoggingManager {
    public void registerTable(String tableName);
    public void addLogRecord(LogRecord logRecord);
}
