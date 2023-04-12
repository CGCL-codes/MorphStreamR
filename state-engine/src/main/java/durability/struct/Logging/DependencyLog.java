package durability.struct.Logging;

import scheduler.struct.MetaTypes;

import java.util.ArrayList;
import java.util.List;

//We use bid instead of LSN
//bid.0 is the first operation in the transaction
//bid.1 is the second operation in the transaction
public class DependencyLog extends CommandLog{
    String id;
    List<String> inEdges = new ArrayList<>();
    List<String> outEdges = new ArrayList<>();
    boolean isAborted = false;
    public DependencyLog(long LSN, String tableName, String key, String OperationFunction, Object parameter) {
        super(LSN, tableName, key, OperationFunction, parameter);
    }
    public void setId(String txn_id){
        this.id = txn_id;
    }
    public void addInEdge(String bid){
        inEdges.add(bid);
    }
    public void addOutEdge(String bid){
        outEdges.add(bid);
    }

    @Override
    public void setVote(MetaTypes.OperationStateType vote) {
        if (vote == MetaTypes.OperationStateType.ABORTED)
            isAborted = true;
    }
    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(id).append(";");//LSN
        for (String id : inEdges) {
            stringBuilder.append(id).append(",");
        }
        stringBuilder.append(";");
        for (String id : outEdges) {
            stringBuilder.append(id).append(",");
        }
        stringBuilder.append(";");
        stringBuilder.append(tableName).append(";");
        stringBuilder.append(key).append(";");
        stringBuilder.append(OperationFunction).append(";");
        stringBuilder.append(parameter).append(";");
        return super.toString();
    }
}
