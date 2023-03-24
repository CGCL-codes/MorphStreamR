package scheduler.struct.recovery;

import transaction.impl.ordered.MyList;

public class OperationChain implements Comparable<OperationChain>{
    private final String tableName;
    private final String primaryKey;
    private final MyList<Operation> operations;
    public OperationChain(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.operations = new MyList<>(tableName, primaryKey);
    }

    @Override
    public int compareTo(OperationChain o) {
        if (o.toString().equals(toString()))
            return 0;
        else
            return -1;
    }
    public String getTableName() {
        return tableName;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }
}
