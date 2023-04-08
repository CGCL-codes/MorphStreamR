package scheduler.context.recovery;

import scheduler.Request;
import scheduler.context.SchedulerContext;
import scheduler.struct.recovery.Operation;
import scheduler.struct.recovery.OperationChain;

import java.util.ArrayDeque;
import java.util.Deque;

public class RSContext implements SchedulerContext {
    public long groupId = 0L;
    public boolean needCheckId = true;
    public int thisThreadId;
    public int totalThreads;
    public ArrayDeque<Request> requests;
    public Deque<OperationChain> allocatedTasks = new ArrayDeque<>();
    public boolean isFinished = false;
    public Operation wait_op;
    public OperationChain ready_oc;
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
    public void next() {
        if (wait_op == null) {
            if (!allocatedTasks.isEmpty()) {
                ready_oc = allocatedTasks.pollLast();
            } else {
                isFinished = true;
            }
        } else {
            allocatedTasks.add(ready_oc);
            ready_oc = wait_op.dependentOC;
            allocatedTasks.remove(wait_op.dependentOC);
            wait_op = null;
        }
    }
}
