package durability.wal.WalEntry;

import java.io.Serializable;
import java.util.Objects;

public class LogRecord implements Serializable {
    public long bid;
    public Update update;
    public LogRecord (long bid, Update update) {
        this.bid = bid;
        this.update = update;
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
        return bid + ";" + update.toString();
    }
}
