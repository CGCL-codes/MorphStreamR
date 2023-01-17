package durability.snapshot;

import utils.FaultToleranceConstants;

public class LoggingOptions {
    private int parallelNum;
    private FaultToleranceConstants.CompressionType compressionAlg;
    public LoggingOptions() {
        parallelNum = 1;
        compressionAlg = FaultToleranceConstants.CompressionType.None;
    }

    public LoggingOptions(int parallelNum, String compressionAlg) {
        this.parallelNum = parallelNum;
        switch(compressionAlg) {
            case "None":
                this.compressionAlg = FaultToleranceConstants.CompressionType.None;
                break;
            case "Snappy":
                this.compressionAlg = FaultToleranceConstants.CompressionType.Snappy;
                break;
            case "XOR":
                this.compressionAlg = FaultToleranceConstants.CompressionType.XOR;
                break;
            case "LZ4":
                this.compressionAlg = FaultToleranceConstants.CompressionType.LZ4;
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
