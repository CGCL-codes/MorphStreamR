package scheduler.context.recovery;

import scheduler.Request;
import scheduler.context.SchedulerContext;
import scheduler.struct.recovery.OperationChain;

import java.util.ArrayDeque;

public class RSContext implements SchedulerContext {
    public int thisThreadId;
    public int totalThreads;
    public ArrayDeque<Request> requests;
    public int scheduledOPs;//current number of operations processed per thread.
    public int totalOsToSchedule;//total number of operations to process per thread.
    public RSContext(int totalThreads) {
        this.totalThreads = totalThreads;
        this.requests = new ArrayDeque<>();

    }
    public OperationChain createTask(String tableName, String primaryKey) {
        return new OperationChain(tableName, primaryKey);
    }
    public void push(Request request) {
        requests.push(request);
    }
}
