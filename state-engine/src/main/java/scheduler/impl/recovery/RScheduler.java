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

public class RScheduler<Context extends RSContext> implements IScheduler {
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
    public void INITIALIZE(Object threadId) {

    }

    @Override
    public void PROCESS(Object threadId, long mark_ID) {

    }

    @Override
    public void EXPLORE(Object o) {

    }

    @Override
    public boolean FINISHED(Object threadId) {
        return false;
    }

    @Override
    public void RESET(Object o) {

    }

    @Override
    public boolean SubmitRequest(Object o, Request request) {
        return false;
    }

    @Override
    public void TxnSubmitBegin(Object o) {

    }

    @Override
    public void TxnSubmitFinished(Object o) {

    }

    @Override
    public void AddContext(int thisTaskId, Object o) {

    }

    @Override
    public void start_evaluation(Object o, long mark_ID, int num_events) {

    }

}
