package durability.wal.WalEntry;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Update implements Serializable {
    private final Map<String, Object> updates = new HashMap<>();

    public void addUpdate(String key, Object update) {
        this.addUpdate(key, update);
    }

    @Override
    public String toString() {
        return updates.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Update updates1= (Update) o;
        return updates.equals(updates1.updates);
    }
}
