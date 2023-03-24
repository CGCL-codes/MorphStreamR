package durability.recovery.histroyviews;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class AbortViews {
    public ConcurrentHashMap<Integer, List<Long>> threadToAbortList = new ConcurrentHashMap<>();
    public void addAbortId(int tid, long bid) {
        if (!threadToAbortList.containsKey(tid))
            threadToAbortList.put(tid, new ArrayList<>());
        threadToAbortList.get(tid).add(bid);
    }
    public boolean inspectView(long bid) {
        for (int tid : threadToAbortList.keySet()) {
            if (threadToAbortList.get(tid).contains(bid))
                return true;
        }
        return false;
    }
}
