package scheduler.impl.og.structured;

import scheduler.context.og.OGSContext;
import scheduler.struct.og.OperationChain;
import utils.SOURCE_CONTROL;

/**
 * The scheduler based on TPG, this is to be invoked when the queue is empty of each thread, it works as follows:
 * 1. explore dependencies in TPG, and find out the ready/speculative operations
 * 2. map operations to threads and put them into the queue of threads.
 * 3. thread will find operations from its queue for execution.
 * It's a shared data structure!
 */
public class OGBFSScheduler extends AbstractOGBFSScheduler<OGSContext> {

    public OGBFSScheduler(int totalThreads, int NUM_ITEMS, int app) {
        super(totalThreads, NUM_ITEMS, app);
    }

    @Override
    public void EXPLORE(OGSContext context) {
        OperationChain next = Next(context);
        while (next == null && !context.exploreFinished()) {
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
            ProcessedToNextLevel(context);
            next = Next(context);
        }
        DISTRIBUTE(next, context);
    }
}