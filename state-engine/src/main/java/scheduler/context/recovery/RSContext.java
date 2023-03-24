package scheduler.context.recovery;

import scheduler.context.SchedulerContext;
import scheduler.struct.recovery.OperationChain;

public class RSContext implements SchedulerContext {
    public int totalThreads;
    public RSContext(int totalThreads) {
        this.totalThreads = totalThreads;
    }
    public OperationChain createTask(String tableName, String primaryKey) {
        return new OperationChain(tableName, primaryKey);
    }
}
