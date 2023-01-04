package utils;

public class FaultToleranceConstants {
    public static int FTOption_ISC = 1;
    public static int FTOption_WSC = 2;
    public enum CompressionType {
        None, FloatMult, BaseDelta, Dictionary, RLE
    }
     public enum FaultToleranceStatus {
         NULL,Undo,Persist,Commit,Snapshot
     }
     public enum Vote{
        Abort, Commit
     }
     public static final int END_OF_TABLE_GROUP_MARK = 0xFFFF;
}
