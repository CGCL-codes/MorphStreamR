package scheduler.impl.recovery;

import durability.logging.LoggingEntry.PathRecord;
import durability.logging.LoggingStrategy.ImplLoggingManager.PathLoggingManager;
import durability.logging.LoggingStrategy.ImplLoggingManager.WALManager;
import durability.logging.LoggingStrategy.LoggingManager;
import scheduler.Request;
import scheduler.context.recovery.RSContext;
import scheduler.impl.IScheduler;
import scheduler.struct.recovery.TaskPrecedenceGraph;
import utils.lib.ConcurrentHashMap;

import static utils.FaultToleranceConstants.*;

public class RScheduler<Context extends RSContext> implements IScheduler<Context> {
    public final int delta;
    public final TaskPrecedenceGraph<Context> tpg;
    public int isLogging;
    public LoggingManager loggingManager;
    public ConcurrentHashMap<Integer, PathRecord> threadToPathRecord;// Used by fault tolerance

    public RScheduler(int totalThreads, int NUM_ITEMS, int app) {
        this.delta = (int) Math.ceil(NUM_ITEMS / (double) totalThreads);
        this.tpg = new TaskPrecedenceGraph<>(totalThreads, delta, NUM_ITEMS, app);
    }
    @Override
    public void initTPG(int offset) {
        tpg.initTPG(offset);
    }
    @Override
    public void setLoggingManager(LoggingManager loggingManager) {
        this.loggingManager = loggingManager;
        if (loggingManager instanceof WALManager) {
            isLogging = LOGOption_wal;
        } else if (loggingManager instanceof PathLoggingManager) {
            isLogging = LOGOption_path;
            this.threadToPathRecord = ((PathLoggingManager) loggingManager).threadToPathRecord;
        } else {
            isLogging = LOGOption_no;
        }
        this.tpg.isLogging = this.isLogging;
    }
    @Override
    public boolean SubmitRequest(Context context, Request request) {
        context.push(request);
        return false;
    }

    @Override
    public void TxnSubmitBegin(Context context) {
        context.requests.clear();
    }

    @Override
    public void TxnSubmitFinished(Context context) {

    }

    @Override
    public void AddContext(int threadId, Context context) {
        tpg.threadToContextMap.put(threadId, context);
        tpg.setOCs(context);
    }

    @Override
    public void INITIALIZE(Context threadId) {
        //TODO:add task placing
    }

    @Override
    public void PROCESS(Context threadId, long mark_ID) {

    }

    @Override
    public void EXPLORE(Context context) {

    }

    @Override
    public boolean FINISHED(Context context) {
        return false;
    }

    @Override
    public void RESET(Context context) {

    }

    @Override
    public void start_evaluation(Context context, long mark_ID, int num_events) {
        INITIALIZE(context);
        do {
            EXPLORE(context);
            PROCESS(context, mark_ID);
        } while (!FINISHED(context));
        RESET(context);
    }

}
