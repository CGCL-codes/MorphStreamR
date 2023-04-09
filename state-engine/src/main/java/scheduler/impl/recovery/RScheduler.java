package scheduler.impl.recovery;

import durability.logging.LoggingStrategy.ImplLoggingManager.PathLoggingManager;
import durability.logging.LoggingStrategy.ImplLoggingManager.WALManager;
import durability.logging.LoggingStrategy.LoggingManager;
import scheduler.Request;
import scheduler.context.recovery.RSContext;
import scheduler.impl.IScheduler;
import scheduler.struct.AbstractOperation;
import scheduler.struct.recovery.Operation;
import scheduler.struct.recovery.OperationChain;
import scheduler.struct.recovery.TaskPrecedenceGraph;
import storage.SchemaRecord;
import storage.datatype.DataBox;
import transaction.function.DEC;
import transaction.function.INC;
import utils.AppConfig;
import utils.SOURCE_CONTROL;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static content.common.CommonMetaTypes.AccessType.*;
import static utils.FaultToleranceConstants.*;

public class RScheduler<Context extends RSContext> implements IScheduler<Context> {
    public final int delta;
    public final TaskPrecedenceGraph<Context> tpg;
    public int isLogging;
    public LoggingManager loggingManager;

    public RScheduler(int totalThreads, int NUM_ITEMS, int app) {
        this.delta = (int) Math.ceil(NUM_ITEMS / (double) totalThreads);
        this.tpg = new TaskPrecedenceGraph<>(totalThreads, delta, NUM_ITEMS, app);
    }
    @Override
    public void initTPG(int offset) {
        tpg.initTPG(offset);
    }
    @Override
    public void setLoggingManager(LoggingManager loggingManager) {
        this.loggingManager = loggingManager;
        if (loggingManager instanceof WALManager) {
            isLogging = LOGOption_wal;
        } else if (loggingManager instanceof PathLoggingManager) {
            isLogging = LOGOption_path;
            this.tpg.threadToPathRecord = ((PathLoggingManager) loggingManager).threadToPathRecord;
        } else {
            isLogging = LOGOption_no;
        }
        this.tpg.isLogging = this.isLogging;
    }
    @Override
    public boolean SubmitRequest(Context context, Request request) {
        context.push(request);
        return false;
    }

    @Override
    public void TxnSubmitBegin(Context context) {
        if (context.needCheckId) {
            context.needCheckId = false;
            context.groupId = this.loggingManager.getHistoryViews().checkGroupId(context.groupId);
        }
        context.requests.clear();
    }

    @Override
    public void TxnSubmitFinished(Context context) {
        int txnOpId = 0;
        for (Request request : context.requests) {
            long bid = request.txn_context.getBID();
            Operation set_op;
            switch (request.accessType) {
                case WRITE_ONLY:
                    set_op = new Operation(request.src_key, getTargetContext(request.src_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, null, null, null, null);
                    set_op.value = request.value;
                    break;
                case READ_WRITE: // they can use the same method for processing
                case READ_WRITE_COND:
                    set_op = new Operation(request.src_key, getTargetContext(request.src_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.function, request.condition, request.condition_records, request.success);
                    break;
                case READ_WRITE_COND_READ:
                case READ_WRITE_COND_READN:
                    set_op = new Operation(request.src_key, getTargetContext(request.src_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.record_ref, request.function, request.condition, request.condition_records, request.success);
                    break;
                case READ_WRITE_READ:
                    set_op = new Operation(request.src_key, getTargetContext(request.src_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.record_ref, request.function, null, null, null);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            //TODO:pre-process dependency
            OperationChain curOC = tpg.addOperationToChain(set_op);
            set_op.setTxnOpId(txnOpId ++);
            if (request.condition_source != null)
                inspectDependency(context.groupId, curOC, set_op, request.table_name, request.src_key, request.condition_sourceTable, request.condition_source);
        }
    }

    @Override
    public void AddContext(int threadId, Context context) {
        tpg.threadToContextMap.put(threadId, context);
        tpg.setOCs(context);
    }

    @Override
    public void INITIALIZE(Context context) {
        //TODO:add task placing
        HashMap<String, List<Integer>> plan = this.loggingManager.inspectTaskPlacing(context.groupId, context.thisThreadId);
        if (plan == null) {
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        } else {
            for (Map.Entry<String, List<Integer>> entry : plan.entrySet()) {
                String table = entry.getKey();
                List<Integer> value = entry.getValue();
                for (int partitionId : value) {
                    OperationChain oc = tpg.getOperationChains().get(table).threadOCsMap.get(partitionId).holder_v1.get(String.valueOf(partitionId));
                    context.allocatedTasks.add(oc);
                }
            }
        }
    }

    @Override
    public void PROCESS(Context context, long mark_ID) {
        OperationChain oc = context.ready_oc;
        boolean continueFlag = true;
        while (continueFlag && oc.operations.size() > 0){
            Operation op = oc.operations.first();
            if (op.pdCount.get() == 0) {
                execute(op, mark_ID, false);
            } else {
                continueFlag = false;
                context.wait_op = op;
            }
        }
    }

    @Override
    public void EXPLORE(Context context) {
        context.next();
    }

    @Override
    public boolean FINISHED(Context context) {
        return context.isFinished;
    }

    @Override
    public void RESET(Context context) {
        context.needCheckId = true;
        context.isFinished = false;
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
    }

    @Override
    public void start_evaluation(Context context, long mark_ID, int num_events) {
        context.groupId = mark_ID;
        INITIALIZE(context);
        do {
            EXPLORE(context);
            PROCESS(context, mark_ID);
        } while (!FINISHED(context));
        RESET(context);
    }
    public Context getTargetContext(String key) {
        // the thread to submit the operation may not be the thread to execute it.
        // we need to find the target context this thread is mapped to.
        int threadId =  Integer.parseInt(key) / delta;
        return tpg.threadToContextMap.get(threadId);
    }
    private void inspectDependency(long groupId, OperationChain curOC, Operation op, String table_name,
                                   String key, String[] condition_sourceTable, String[] condition_source){
        if (condition_source != null) {
            for (int index = 0; index < condition_source.length; index++) {
                if (table_name.equals(condition_sourceTable[index]) && key.equals(condition_source[index]))
                    continue;
                Object history = this.loggingManager.inspectDependencyView(groupId, table_name, key,condition_source[index], op.bid);
                if (history != null) {
                    op.historyView = history;
                } else {
                    OperationChain OCFromConditionSource = tpg.getOC(condition_sourceTable[index], condition_source[index]);
                    //DFS-like
                    op.incrementPd(OCFromConditionSource);
                    //Add the proxy operations
                    OCFromConditionSource.addOperation(op);
                    //Add dependent
                    OCFromConditionSource.addDependentOCs(curOC);
                }
            }
        }
    }
    public void execute(Operation operation, long mark_ID, boolean clean) {
        if (operation.accessType.equals(READ_WRITE_COND_READ)) {
            Transfer_Fun(operation, mark_ID, clean);
            // check whether it needs to return a read results of the operation
            if (operation.record_ref != null) {
                operation.record_ref.setRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
            }
            // operation success check, number of operation succeeded does not increase after execution

        } else if (operation.accessType.equals(READ_WRITE_COND)) {
            if (this.tpg.getApp() == 1) {//SL
                Transfer_Fun(operation, mark_ID, clean);
            } else {//OB
                AppConfig.randomDelay();
                List<DataBox> d_record = operation.condition_records[0].content_.ReadAccess(operation.bid, mark_ID, clean, operation.accessType).getValues();
                long askPrice = d_record.get(1).getLong();//price
                long left_qty = d_record.get(2).getLong();//available qty;
                long bidPrice = operation.condition.arg1;
                long bid_qty = operation.condition.arg2;
                if (bidPrice > askPrice || bid_qty < left_qty) {
                    d_record.get(2).setLong(left_qty - operation.function.delta_long);//new quantity.
                    operation.success[0]++;
                }
            }

        } else if (operation.accessType.equals(READ_WRITE)) {
            if (this.tpg.getApp() == 1) {
                Depo_Fun(operation, mark_ID, clean);
            } else {
                AppConfig.randomDelay();
                SchemaRecord srcRecord = operation.s_record.content_.ReadAccess(operation.bid,mark_ID,clean,operation.accessType);
                List<DataBox> values = srcRecord.getValues();
                if (operation.function instanceof INC) {
                    values.get(2).setLong(values.get(2).getLong() + operation.function.delta_long);
                } else
                    throw new UnsupportedOperationException();
            }
        }
    }
    protected void Transfer_Fun(AbstractOperation operation, long previous_mark_ID, boolean clean) {
        Operation op = (Operation) operation;
        final long sourceAccountBalance;
        if (op.historyView != null) {
            SchemaRecord preValues = operation.condition_records[0].content_.readPreValues(operation.bid);
            sourceAccountBalance = preValues.getValues().get(1).getLong();
        } else {
            sourceAccountBalance = (long) op.historyView;
        }
        // apply function
        AppConfig.randomDelay();

        if (sourceAccountBalance > operation.condition.arg1
                && sourceAccountBalance > operation.condition.arg2) {
            // read
            SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record

            if (operation.function instanceof INC) {
                tempo_record.getValues().get(1).incLong(sourceAccountBalance, operation.function.delta_long);//compute.
            } else if (operation.function instanceof DEC) {
                tempo_record.getValues().get(1).decLong(sourceAccountBalance, operation.function.delta_long);//compute.
            } else
                throw new UnsupportedOperationException();
            operation.d_record.content_.updateMultiValues(operation.bid, previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
            synchronized (operation.success) {
                operation.success[0]++;
            }
        }
    }
    protected void Depo_Fun(AbstractOperation operation, long mark_ID, boolean clean) {
        SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
        List<DataBox> values = srcRecord.getValues();
        //apply function to modify..
        AppConfig.randomDelay();
        SchemaRecord tempo_record;
        tempo_record = new SchemaRecord(values);//tempo record
        tempo_record.getValues().get(1).incLong(operation.function.delta_long);//compute.
        operation.s_record.content_.updateMultiValues(operation.bid, mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
    }
}
