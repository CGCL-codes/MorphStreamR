package scheduler.struct.recovery;

import java.util.ArrayDeque;

public class Task{
    public int weight = 0;
    ArrayDeque<OperationChain> ocs = new ArrayDeque<>();
    public void add(OperationChain oc) {
        ocs.add(oc);
    }
}
