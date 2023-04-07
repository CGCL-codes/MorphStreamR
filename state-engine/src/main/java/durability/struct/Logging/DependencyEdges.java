package durability.struct.Logging;
import java.io.Serializable;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DependencyEdges implements Serializable {
    public ConcurrentHashMap<String, AtomicInteger> edgesWeights = new ConcurrentHashMap<>();//<to, weight>
    public ConcurrentHashMap<String, Vector<DependencyResult>> dependencyEdges = new ConcurrentHashMap<>();//<to, DependencyResults>
    public void addEdges(String to, long bid, Object result) {
        this.edgesWeights.putIfAbsent(to, new AtomicInteger(0));
        this.edgesWeights.get(to).getAndIncrement();
        this.dependencyEdges.putIfAbsent(to, new Vector<>());
        this.dependencyEdges.get(to).add(new DependencyResult(bid, result));
    }
    public void clean() {
        this.edgesWeights.clear();
        this.dependencyEdges.clear();
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, Vector<DependencyResult>> des:this.dependencyEdges.entrySet()) {
            stringBuilder.append(des.getKey());
            stringBuilder.append(",");
            for (DependencyResult result : des.getValue()) {
                stringBuilder.append(result.toString());
                stringBuilder.append(",");
            }
            stringBuilder.append(":");
        }
        return stringBuilder.toString();
    }
}
