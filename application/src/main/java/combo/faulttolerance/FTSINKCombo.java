package combo.faulttolerance;

import combo.SINKCombo;
import execution.runtime.tuple.impl.Tuple;
import profiler.Metrics;

public class FTSINKCombo extends SINKCombo {
    long lastTask = -1;

    @Override
    public void execute(Tuple input) throws InterruptedException {
        if (input.getBID() > lastTask) {
            Metrics.RuntimePerformance.lastTasks[this.thisTaskId] = input.getBID();
            latency_measure(input);
        }
    }
}
