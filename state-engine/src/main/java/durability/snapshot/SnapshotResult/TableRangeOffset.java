package durability.snapshot.SnapshotResult;

import java.io.Serializable;

public class TableRangeOffset implements Serializable {
    private final long[] offsets;

    public TableRangeOffset() {
        offsets = new long[0];
    }
}
