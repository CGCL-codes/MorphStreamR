package scheduler.impl.recovery;

import durability.logging.LoggingStrategy.LoggingManager;
import scheduler.Request;
import scheduler.impl.IScheduler;

public class RScheduler implements IScheduler {
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

    @Override
    public void initTPG(int offset) {

    }

    @Override
    public void setLoggingManager(LoggingManager loggingManager) {

    }
}
