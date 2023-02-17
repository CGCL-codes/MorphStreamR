package durability.logging.LoggingEntry;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PathRecord implements Serializable {
    public List<Long> abortBids = new ArrayList<>();

    public void addAbortBid(long bid) {
        if (abortBids.contains(bid))
            return;
        abortBids.add(bid);
    }
    public void reset() {
        this.abortBids.clear();
    }
}
