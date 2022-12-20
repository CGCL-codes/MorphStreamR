package durability.snapshot;

import utils.FaultToleranceConstants;

public class SnapshotOptions {
    private int parallelNum;
    private FaultToleranceConstants.CompressionType compressionAlg;
    public SnapshotOptions() {
        parallelNum = 1;
        compressionAlg = FaultToleranceConstants.CompressionType.None;
    }

    public SnapshotOptions(int parallelNum, String compressionAlg) {
        this.parallelNum = parallelNum;
        switch(compressionAlg) {
            case "None":
                this.compressionAlg = FaultToleranceConstants.CompressionType.None;
                break;
            case "FloatMult":
                this.compressionAlg = FaultToleranceConstants.CompressionType.FloatMult;
                break;
            case "BaseDelta":
                this.compressionAlg = FaultToleranceConstants.CompressionType.BaseDelta;
                break;
            case "Dictionary":
                this.compressionAlg = FaultToleranceConstants.CompressionType.Dictionary;
                break;
            case "RLE":
                this.compressionAlg = FaultToleranceConstants.CompressionType.RLE;
                break;
        }

    }
    public int getParallelNum() {
        return parallelNum;
    }

    public FaultToleranceConstants.CompressionType getCompressionAlg() {
        return compressionAlg;
    }
}
