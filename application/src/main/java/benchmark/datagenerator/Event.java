package benchmark.datagenerator;

public abstract class Event {
    public long timestamp;
    public String toString(int iterationNumber, int totalTransaction) {
        throw new UnsupportedOperationException("unsupported by abstract class");
    }
}
