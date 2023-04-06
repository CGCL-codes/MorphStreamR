package durability.recovery.histroyviews;
import java.util.concurrent.ConcurrentHashMap;

public class HistoryViews {
    public AbortViews abortViews = new AbortViews();
    public ConcurrentHashMap<Long, DependencyViews> groupToDependencyView = new ConcurrentHashMap<>();
    public void addAbortId(int threadId, long bid) {
        abortViews.addAbortId(threadId, bid);
    }
    public void addDependencies(long groupId, String key, long bid, Object v) {
        if (!groupToDependencyView.containsKey(groupId))
            groupToDependencyView.put(groupId, new DependencyViews());
        groupToDependencyView.get(groupId).addDependencies(key, bid, v);
    }
    public boolean inspectAbortView(long bid) {
        return abortViews.inspectView(bid);
    }
    public Object inspectDependencyView(long groupId, String key, long bid) {
        if (!groupToDependencyView.containsKey(groupId))
            return null;
        return groupToDependencyView.get(groupId).inspectView(key, bid);
    }
}
