package utils;

public class FaultToleranceConstants {
    public enum CompressionType {
        None, FloatMult, BaseDelta, Dictionary, RLE
    }
     public enum FaultToleranceStatus {
         NULL,Undo,Persist,Recovery,Snapshot
     }
}
