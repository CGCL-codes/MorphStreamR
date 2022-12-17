package durability.snapshot;

public class SnapshotOptions {
    private int parallelNum;
    private String compressionAlg;
    public SnapshotOptions() {
        parallelNum = 1;
        compressionAlg = null;
    }

    public SnapshotOptions(int parallelNum, String compressionAlg) {
        this.parallelNum = parallelNum;
        this.compressionAlg = compressionAlg;
    }
    public int getParallelNum() {
        return parallelNum;
    }

    public String getCompressionAlg() {
        return compressionAlg;
    }
}
