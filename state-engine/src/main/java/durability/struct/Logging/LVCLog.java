package durability.struct.Logging;

public class LVCLog extends CommandLog{
    private int[] LVs;
    public LVCLog(long LSN, String tableName, String key, String OperationFunction, Object parameter) {
        super(LSN, tableName, key, OperationFunction, parameter);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
