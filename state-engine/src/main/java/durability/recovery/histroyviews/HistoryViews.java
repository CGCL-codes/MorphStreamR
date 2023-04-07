package durability.recovery.histroyviews;
import java.util.concurrent.ConcurrentHashMap;

public class HistoryViews {
    public AbortViews abortViews = new AbortViews();
    public ConcurrentHashMap<Long, DependencyViews> groupToDependencyView = new ConcurrentHashMap<>();
    public void addAbortId(int threadId, long bid) {
        abortViews.addAbortId(threadId, bid);
    }
    public void addDependencies(long groupId, String table, String from, String to, long bid, Object v) {
        if (!groupToDependencyView.containsKey(groupId))
            groupToDependencyView.put(groupId, new DependencyViews());
        groupToDependencyView.get(groupId).addDependencies(table, from, to, bid, v);
    }
    public boolean inspectAbortView(long bid) {
        return abortViews.inspectView(bid);
    }
    public Object inspectDependencyView(String table, String from, String to, long bid) {
        long groupId = checkGroupId(bid);
        if (!groupToDependencyView.containsKey(groupId))
            return null;
        return groupToDependencyView.get(groupId).inspectView(table, from, to, bid);
    }
    private long checkGroupId(long bid) {
        for (long groupId : this.groupToDependencyView.keySet()) {
            if (groupId > bid) {
                return groupId;
            }
        }
        return 0L;
    }
}
