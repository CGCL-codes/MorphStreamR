package utils;

public class FaultToleranceConstants {
    public enum CompressionType {
        None, FloatMult, BaseDelta, Dictionary, RLE
    }
     public enum FaultToleranceStatus {
         NULL,Undo,Persist,Recovery,Snapshot
     }
     public static final int END_OF_TABLE_GROUP_MARK = 0xFFFF;
}
