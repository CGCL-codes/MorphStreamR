package durability.recovery.histroyviews;

import utils.lib.ConcurrentHashMap;

import java.util.HashMap;
import java.util.List;

public class AllocationPlan {
    public ConcurrentHashMap<String, ConcurrentHashMap<Integer, List<Integer>>> tableToPlan;
    public List<List<Integer>> allocationPlan;
    public HashMap<String, List<Integer>> getPlanByThreadId(int threadId) {
        HashMap<String, List<Integer>> plan = new HashMap<>();
        for (String table : tableToPlan.keySet()) {
            plan.put(table, tableToPlan.get(table).get(threadId));
        }
        return plan;
    }
}
