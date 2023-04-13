package durability.recovery.lsnvector;

import durability.struct.Logging.LVCLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.lib.ConcurrentHashMap;

import java.util.Arrays;

public class CommandPrecedenceGraph {
    private static final Logger LOG = LoggerFactory.getLogger(CommandPrecedenceGraph.class);
    public int[] GlobalLV;
    public ConcurrentHashMap<Integer, CSContext> threadToCSContextMap = new ConcurrentHashMap<>();
    public void addContext(int threadId, CSContext context) {
        threadToCSContextMap.put(threadId, context);
    }
    public void addTask(int threadId, LVCLog task) {
        threadToCSContextMap.get(threadId).addTask(task);
    }
    public CSContext getContext(int threadId) {
        return threadToCSContextMap.get(threadId);
    }
    public void init_globalLv(CSContext context) {
        context.totalTaskCount = context.tasks.size();
        if (context.threadId == 0) {
            GlobalLV = new int[threadToCSContextMap.size()];
            Arrays.fill(GlobalLV, 0);
        }
    }
    public void updateGlobalLV(CSContext context) {
        this.GlobalLV[context.threadId] = context.readyTask.getLSN() + 1;
    }
    public void reset(CSContext context) {
        context.reset();
    }

    public boolean canEvaluate(LVCLog lvcLog) {
        for (int i = 0; i < GlobalLV.length; i++) {
            if (lvcLog.getLVs()[i] > GlobalLV[i])
                return false;
        }
        return true;
    }

}
