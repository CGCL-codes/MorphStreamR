package durability.inputStore;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Queue;

public abstract class InputReload {
    public int partitionOffset;
    public int tthread;
    public abstract void reloadInput(BufferedReader bufferedReader, Queue<Object> lostEvents) throws IOException;
}
