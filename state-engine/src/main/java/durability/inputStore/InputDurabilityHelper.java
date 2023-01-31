package durability.inputStore;

import utils.FaultToleranceConstants;

import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

public abstract class InputDurabilityHelper {
    public int partitionOffset;
    public int tthread;
    public int taskId;
    public FaultToleranceConstants.CompressionType encodingType;
    public boolean isCompression = true;

    public abstract void reloadInput(File inputFile, Queue<Object> lostEvents, long redoOffset) throws IOException, ExecutionException, InterruptedException;
    public abstract void storeInput(Object[] myevents, long currentOffset, int interval, String inputStoreCurrentPath) throws IOException, ExecutionException, InterruptedException;
}
