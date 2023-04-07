package scheduler.struct.recovery;

import transaction.impl.ordered.MyList;

import java.util.Vector;

public class OperationChain implements Comparable<OperationChain>{
    private final String tableName;
    private final String primaryKey;
    private final MyList<Operation> operations;
    private final Vector<OperationChain> dependentOCs = new Vector<>();
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

    public void addOperation(Operation operation) {
        operations.add(operation);
    }
    public void addDependentOCs(OperationChain ocs) {
        if (this.dependentOCs.contains(ocs))
            return;
        this.dependentOCs.add(ocs);
    }
}
