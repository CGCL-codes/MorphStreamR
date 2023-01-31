package utils;

public class FaultToleranceConstants {
    public static int FTOption_ISC = 1;
    public static int FTOption_WSC = 2;
    /**
     * XOR GorillaV2
     * Delta-Delta GorillaV1
     * Delta DeltaBinary
     * Rle Rle
     * Dictionary Dictionary
     * Zigzag Zigzag
     */
    public enum CompressionType {
        None, XOR, Delta2Delta, Delta, RLE, Dictionary, Snappy, Zigzag
    }
     public enum FaultToleranceStatus {
         NULL,Undo,Persist,Commit,Snapshot
     }
     public enum Vote{
        Abort, Commit
     }
     public static final int END_OF_TABLE_GROUP_MARK = 0xFFFF;
}
