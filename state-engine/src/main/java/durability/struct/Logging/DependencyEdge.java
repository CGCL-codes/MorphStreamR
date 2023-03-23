package durability.struct.Logging;

import java.io.Serializable;

public class DependencyEdge implements Serializable {
    public long bid;
    public Object value;
    public DependencyEdge(long bid, Object value) {
        this.bid = bid;
        this.value = value;
    }
}
