package scheduler.struct.recovery;

import java.util.ArrayDeque;

public class Tasks {
    public ArrayDeque<Task> tasks = new ArrayDeque<>();
    public Tasks(Task task) {
        this.tasks.add(task);
    }
}
