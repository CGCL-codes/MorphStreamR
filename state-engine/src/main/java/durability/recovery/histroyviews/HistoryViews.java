package durability.recovery.histroyviews;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HistoryViews {
    public ConcurrentHashMap<Long, AllocationPlan> allocationPlans = new ConcurrentHashMap<>();//Group to allocationPlan
    public AbortViews abortViews = new AbortViews();
    public ConcurrentHashMap<Long, DependencyViews> groupToDependencyView = new ConcurrentHashMap<>();//Group to DependencyView
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
    public Object inspectDependencyView(long groupId, String table, String from, String to, long bid) {
        if (!groupToDependencyView.containsKey(groupId))
            return null;
        return groupToDependencyView.get(groupId).inspectView(table, from, to, bid);
    }
    public HashMap<String, List<Integer>> inspectTaskPlacing(long groupId, int threadId) {
        if (!allocationPlans.containsKey(groupId))
            return null;
        return allocationPlans.get(groupId).getPlanByThreadId(threadId);
    }
    public long checkGroupId(long curId) {
        for (long groupId : this.groupToDependencyView.keySet()) {
            if (groupId >= curId) {
                return groupId;
            }
        }
        return 0L;
    }
}
