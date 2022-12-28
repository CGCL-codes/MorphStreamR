package components.operators.api;

import java.util.concurrent.BrokenBarrierException;

public interface Checkpointable {
    // punctuation count
    boolean model_switch(int counter) throws InterruptedException, BrokenBarrierException;

}
