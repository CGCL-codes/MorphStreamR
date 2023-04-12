package durability.struct.Logging;

public abstract class CommandLog implements LoggingEntry, Comparable<CommandLog> {
    private long LSN;
    public String tableName;
    public String key;
    public String OperationFunction;
    public Object parameter;
    public CommandLog(long LSN, String tableName, String key, String OperationFunction, Object parameter){
        this.LSN = LSN;
        this.tableName = tableName;
        this.key = key;
        this.OperationFunction = OperationFunction;
        this.parameter = parameter;
    }

    @Override
    public int compareTo(CommandLog o) {
        return Long.compare(this.LSN, o.LSN);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
