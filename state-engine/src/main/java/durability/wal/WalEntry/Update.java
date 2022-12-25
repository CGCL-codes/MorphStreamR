package durability.wal.WalEntry;

import java.io.Serializable;
import java.util.Objects;

public class Update implements Serializable, Comparable {
    private Object update;
    private final long bid;

    private final String key;

    public Update(long bid, String key) {
        this.bid = bid;
        this.key = key;
    }

    public void addUpdate(Object update) {
        this.update = update;
    }

    @Override
    public String toString() {
        return bid + "," + key + "," + update.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Update updates1= (Update) o;
        if (bid != updates1.bid) return false;
        if (!Objects.equals(key, updates1.key)) return false;
        return update.equals(updates1.update);
    }

    /**
     * First compare bid, if they have the same bid then compare key
     * */
    @Override
    public int compareTo(Object obj) {
        if (this.getClass() != obj.getClass()) {
            return 0;
        }
        Update other = (Update) obj;
        if (this.bid != other.bid) {
            return Long.compare(this.bid, other.bid);
        } else {
            return Integer.compare(Integer.parseInt(key), Integer.parseInt(other.key));
        }
    }
}
