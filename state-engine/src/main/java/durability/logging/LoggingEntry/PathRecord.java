package durability.logging.LoggingEntry;

import durability.struct.Logging.DependencyEdge;
import durability.struct.Logging.LoggingEntry;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PathRecord implements LoggingEntry {
    public List<Long> abortBids = new ArrayList<>();
    public HashMap<String, List<DependencyEdge>> dependencyEdges = new HashMap<>();//<Key, bid>
    public void addAbortBid(long bid) {
        if (abortBids.contains(bid))
            return;
        abortBids.add(bid);
    }
    public void addDependencyEdge(String key, long bid, Object value) {
        if (!dependencyEdges.containsKey(key))
            dependencyEdges.put(key, new ArrayList<>());
        dependencyEdges.get(key).add(new DependencyEdge(bid, value));
    }
    public void reset() {
        this.abortBids.clear();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(abortBids.size()).append(" ");
        for (long bid : abortBids) {
            sb.append(bid).append(" ");
        }
        for (String key : dependencyEdges.keySet()) {
            sb.append(key).append(" ");
            sb.append(dependencyEdges.get(key).size()).append(" ");
            for (DependencyEdge edge : dependencyEdges.get(key)) {
                sb.append(edge.bid).append(" ");
            }
        }
        return sb.toString();
    }
}
