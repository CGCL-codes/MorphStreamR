package durability.logging.LoggingEntry;

import common.util.graph.Edge;
import common.util.graph.Graph;
import durability.struct.Logging.Node;
import durability.struct.Logging.LoggingEntry;
import durability.struct.Logging.keyToDependencies;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PathRecord implements LoggingEntry {
    public List<Long> abortBids = new ArrayList<>();
    public ConcurrentHashMap<String, List<Integer>> tableToPlacing = new ConcurrentHashMap<>();//<Table, Placing>
    public ConcurrentHashMap<String, keyToDependencies> dependencyEdges = new ConcurrentHashMap<>();//<Table, DependencyEdge>
    public void addAbortBid(long bid) {
        if (abortBids.contains(bid))
            return;
        abortBids.add(bid);
    }
    public void addDependencyEdge(String table, String from, String to, long bid, Object value) {
        dependencyEdges.putIfAbsent(table, new keyToDependencies());
        dependencyEdges.get(table).addDependencies(from, to, bid, value);
    }
    public void addNode(String table, String from, int weight) {
        dependencyEdges.putIfAbsent(table, new keyToDependencies());
        dependencyEdges.get(table).holder.putIfAbsent(from, new Node(from));
        dependencyEdges.get(table).holder.get(from).setNodeWeight(weight);
    }
    public void reset() {
        this.abortBids.clear();
        this.tableToPlacing.clear();
        for (keyToDependencies edges : this.dependencyEdges.values()) {
            edges.cleanDependency();
        }
    }
    public void dependencyToGraph(Graph graph, String table){
        keyToDependencies partitionG = this.dependencyEdges.get(table);
        for (Node node : partitionG.holder.values()) {
            graph.addNode(node.from, node.weight);
            for (Edge edge : node.edges.values()) {
                graph.addEdge(edge);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<Integer>> allocation : this.tableToPlacing.entrySet()) {
            sb.append(allocation.getKey());
            sb.append(":");
            for (int pid : allocation.getValue()) {
                sb.append(pid);
                sb.append(":");
            }
            sb.append(";");
        }
        sb.append(" ");
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
