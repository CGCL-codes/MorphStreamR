package scheduler.struct.recovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scheduler.context.recovery.RSContext;
import utils.lib.ConcurrentHashMap;

import java.util.Deque;

public class TaskPrecedenceGraph <Context extends RSContext>{
    private static final Logger LOG = LoggerFactory.getLogger(TaskPrecedenceGraph.class);
    public final int totalThreads;
    protected final int delta;//range of each partition. depends on the number of op in the stage.
    private final int NUM_ITEMS;
    private final int app;
    public int isLogging;
    public final ConcurrentHashMap<Integer, Context> threadToContextMap;
    private final ConcurrentHashMap<String, TableOCs> operationChains;//shared data structure.
    public final ConcurrentHashMap<Integer, Deque<OperationChain>> threadToOCs;//Exactly which OCs are executed by each thread.

    public TaskPrecedenceGraph(int totalThreads, int delta, int NUM_ITEMS, int app) {
        this.totalThreads = totalThreads;
        this.delta = delta;
        this.NUM_ITEMS = NUM_ITEMS;
        this.app = app;
        this.threadToContextMap = new ConcurrentHashMap<>();
        this.operationChains = new ConcurrentHashMap<>();
        this.threadToOCs = new ConcurrentHashMap<>();
    }
    public void initTPG(int offset) {
        if (app == 0) {//GS
            operationChains.put("MicroTable", new TableOCs(totalThreads,offset));
        } else if (app == 1) {//SL
            operationChains.put("accounts", new TableOCs(totalThreads,offset));
            operationChains.put("bookEntries", new TableOCs(totalThreads,offset));
        } else if (app == 2){//TP
            operationChains.put("segment_speed",new TableOCs(totalThreads,offset));
            operationChains.put("segment_cnt",new TableOCs(totalThreads,offset));
        } else if (app == 3) {//OB
            operationChains.put("goods",new TableOCs(totalThreads,offset));
        }
    }
    public void setOCs(Context context) {

    }
    private void resetOCs(){

    }
    public TableOCs getTableOCs(String tableName) {
        return operationChains.get(tableName);
    }
    public ConcurrentHashMap<String, TableOCs> getOperationChains() {
        return operationChains;
    }
    private OperationChain getOC(String tableName, String pKey) {
        int threadId = Integer.parseInt(pKey) / delta;
        ConcurrentHashMap<String, OperationChain> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
        return holder.computeIfAbsent(pKey, s -> threadToContextMap.get(threadId).createTask(tableName, pKey));
    }
    private OperationChain getOC(String tableName, String pKey, int threadId) {
        ConcurrentHashMap<String, OperationChain> holder = getTableOCs(tableName).threadOCsMap.get(threadId).holder_v1;
        return holder.computeIfAbsent(pKey, s -> threadToContextMap.get(threadId).createTask(tableName, pKey));
    }

}
