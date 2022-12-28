package combo.faulttolerance;

import java.util.concurrent.BrokenBarrierException;

public interface FaultTolerance {
    boolean snapshot(int counter) throws InterruptedException, BrokenBarrierException;

    boolean input_store();
}
