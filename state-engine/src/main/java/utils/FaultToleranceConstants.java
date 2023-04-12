package utils;

public class FaultToleranceConstants {
    public static int FTOption_ISC = 1;
    public static int FTOption_WSC = 2;
    public static int FTOption_PATH = 3;
    public static int FTOption_LVC = 4;
    public static int FTOption_Dependency = 5;
    public static int LOGOption_no = 0;
    public static int LOGOption_wal = 1;
    public static int LOGOption_path = 2;
    public static int LOGOption_lv = 3;
    public static int LOGOption_dependency = 4;
    /**
     * XOR GorillaV2
     * Delta-Delta GorillaV1
     * Delta DeltaBinary
     * Rle Rle
     * Dictionary Dictionary
     * Zigzag Zigzag
     */
    public enum CompressionType {
        None, XOR, Delta2Delta, Delta, RLE, Dictionary, Snappy, Zigzag, Optimize
    }
     public enum FaultToleranceStatus {
         NULL,Undo,Persist,Commit,Snapshot
     }
     public enum Vote{
        Abort, Commit
     }
     public static final int END_OF_TABLE_GROUP_MARK = 0xFFFF;
}
