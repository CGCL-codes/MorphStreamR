package durability.recovery.histroyviews;
import java.util.concurrent.ConcurrentHashMap;

public class HistoryViews {
    public ConcurrentHashMap<Long, AbortViews> groupToAbortView = new ConcurrentHashMap<>();
    public ConcurrentHashMap<Long, DependencyViews> groupToDependencyView = new ConcurrentHashMap<>();
    public void addAbortId(long groupId, int threadId, long bid) {
        if (!groupToAbortView.containsKey(groupId))
            groupToAbortView.put(groupId, new AbortViews());
        groupToAbortView.get(groupId).addAbortId(threadId, bid);
    }
    public void addDependencies(long groupId, String key, long bid, Object v) {
        if (!groupToDependencyView.containsKey(groupId))
            groupToDependencyView.put(groupId, new DependencyViews());
        groupToDependencyView.get(groupId).addDependencies(key, bid, v);
    }
    public boolean inspectAbortView(long groupId, long bid) {
        if (!groupToAbortView.containsKey(groupId))
            return false;
        return groupToAbortView.get(groupId).inspectView(bid);
    }
    public Object inspectDependencyView(long groupId, String key, long bid) {
        if (!groupToDependencyView.containsKey(groupId))
            return null;
        return groupToDependencyView.get(groupId).inspectView(key, bid);
    }
}
