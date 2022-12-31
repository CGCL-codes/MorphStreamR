package combo.faulttolerance;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;

public interface FaultTolerance {
    boolean snapshot(int counter) throws InterruptedException, BrokenBarrierException;

    boolean input_store(long currentOffset) throws IOException;
}
