package durability.logging.LoggingEntry;

import durability.struct.Logging.LoggingEntry;
import durability.struct.Logging.keyToDependencies;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PathRecord implements LoggingEntry {
    public List<Long> abortBids = new ArrayList<>();
    public ConcurrentHashMap<String, keyToDependencies> dependencyEdges = new ConcurrentHashMap<>();//<Key, DependencyEdge>
    public void addAbortBid(long bid) {
        if (abortBids.contains(bid))
            return;
        abortBids.add(bid);
    }
    public void addDependencyEdge(String table, String from, String to, long bid, Object value) {
        dependencyEdges.putIfAbsent(table, new keyToDependencies());
        dependencyEdges.get(table).addDependencies(from, to, bid, value);
    }
    public void reset() {
        this.abortBids.clear();
        this.dependencyEdges.clear();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(abortBids.size()).append(";");
        for (long bid : abortBids) {
            sb.append(bid).append(";");
        }
        sb.append(" ");
        for (Map.Entry<String, keyToDependencies> logs : this.dependencyEdges.entrySet()) {
            sb.append(logs.getKey());
            sb.append(";");
            sb.append(logs.getValue().toString());
            sb.append(" ");
        }
        return sb.toString();
    }
}
