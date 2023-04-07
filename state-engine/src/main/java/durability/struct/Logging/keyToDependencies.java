package durability.struct.Logging;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class keyToDependencies implements Serializable {
    public ConcurrentHashMap<String, DependencyEdges> holder = new ConcurrentHashMap<>();
    public void addDependencies(String from, String to, long bid, Object v) {
        this.holder.putIfAbsent(from, new DependencyEdges());
        this.holder.get(from).addEdges(to, bid, v);
    }
    public void cleanDependency() {
        for (DependencyEdges edges : this.holder.values()) {
            edges.clean();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, DependencyEdges> ds : holder.entrySet()) {
            sb.append(ds.getKey());
            sb.append(":");
            sb.append(ds.getValue().toString());
            sb.append(";");
        }
        return sb.toString();
    }
}
