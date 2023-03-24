package durability.recovery.histroyviews;

import java.util.concurrent.ConcurrentHashMap;

public class DependencyViews {
    public ConcurrentHashMap<String, Dependencies> keyToDependencies = new ConcurrentHashMap<>();
    public void addDependencies(String key, long bid, Object v) {
        if (!keyToDependencies.containsKey(key))
            keyToDependencies.put(key, new Dependencies());
        keyToDependencies.get(key).addDependency(bid, v);
    }
    public Object inspectView(String key, long bid) {
        if (!keyToDependencies.containsKey(key))
            return null;
        return keyToDependencies.get(key).dependencies.get(bid);
    }
}
