package durability.logging.LoggingEntry;

import java.io.Serializable;
import java.util.Objects;

public class LogRecord implements Serializable {
    public String tableName;
    public int partitionId;
    public long bid;
    public Update update;
    public LogRecord (String tableName, int partitionId, long bid, String key) {
        this.bid = bid;
        this.tableName = tableName;
        this.update = new Update(bid, key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogRecord entry = (LogRecord) o;

        if (bid != entry.bid) return false;
        return Objects.equals(update, entry.update);
    }

    public String toString() {
        return tableName + ";" + update.toString();
    }

}
