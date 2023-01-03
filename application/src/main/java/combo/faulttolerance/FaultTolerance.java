package combo.faulttolerance;

import java.io.IOException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

public interface FaultTolerance {
    boolean snapshot(int counter) throws InterruptedException, BrokenBarrierException;

    boolean input_store(long currentOffset) throws IOException;

    boolean input_reload(long snapshotOffset, long redoOffset) throws IOException;
    boolean recoverData() throws IOException, ExecutionException, InterruptedException;
}
