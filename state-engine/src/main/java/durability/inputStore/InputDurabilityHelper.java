package durability.inputStore;

import common.io.Compressor.Compressor;
import common.io.Enums.Encoding;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Queue;

public abstract class InputDurabilityHelper {
    public int partitionOffset;
    public int tthread;
    public int taskId;
    public Encoding encodingType;

    public abstract void reloadInput(BufferedReader bufferedReader, Queue<Object> lostEvents, long redoOffset) throws IOException;
    public abstract void storeInput(Object[] myevents, long currentOffset, int interval, String inputStoreCurrentPath);
}
