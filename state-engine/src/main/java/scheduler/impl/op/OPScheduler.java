package scheduler.impl.op;


import durability.logging.LoggingEntry.LogRecord;
import durability.logging.LoggingStrategy.ImplLoggingManager.*;
import durability.logging.LoggingStrategy.LoggingManager;
import durability.struct.Logging.DependencyLog;
import durability.struct.Logging.LVCLog;
import durability.struct.Logging.NativeCommandLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.impl.IScheduler;
import scheduler.context.op.OPSchedulerContext;
import scheduler.struct.AbstractOperation;
import scheduler.struct.MetaTypes;
import scheduler.struct.op.Operation;
import scheduler.struct.op.TaskPrecedenceGraph;
import storage.SchemaRecord;
import storage.TableRecord;
import storage.datatype.DataBox;
import storage.datatype.DoubleDataBox;
import storage.datatype.IntDataBox;
import transaction.function.AVG;
import transaction.function.DEC;
import transaction.function.INC;
import transaction.function.SUM;
import utils.AppConfig;
import utils.SOURCE_CONTROL;

import java.util.HashSet;
import java.util.List;

import static common.constants.TPConstants.Constant.MAX_INT;
import static common.constants.TPConstants.Constant.MAX_SPEED;
import static content.common.CommonMetaTypes.AccessType.*;
import static utils.FaultToleranceConstants.*;

public abstract class OPScheduler<Context extends OPSchedulerContext, Task> implements IScheduler<Context> {
    private static final Logger log = LoggerFactory.getLogger(OPScheduler.class);
    public final int delta;//range of each partition. depends on the number of op in the stage.
    public final TaskPrecedenceGraph<Context> tpg; // TPG to be maintained in this global instance.
    public LoggingManager loggingManager; // Used by fault tolerance
    public int isLogging;// Used by fault tolerance
    public OPScheduler(int totalThreads, int NUM_ITEMS, int app) {
        delta = (int) Math.ceil(NUM_ITEMS / (double) totalThreads); // Check id generation in DateGenerator.
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
        } else if (loggingManager instanceof DependencyLoggingManager) {
            isLogging = LOGOption_dependency;
        } else if (loggingManager instanceof LSNVectorLoggingManager) {
            isLogging = LOGOption_lv;
        } else if (loggingManager instanceof CommandLoggingManager) {
            isLogging = LOGOption_command;
        } else {
            isLogging = LOGOption_no;
        }
        this.tpg.isLogging = this.isLogging;
    }

    /**
     * state to thread mapping
     *
     * @param key
     * @param delta
     * @return
     */
    public static int getTaskId(String key, Integer delta) {
        Integer _key = Integer.valueOf(key);
        return _key / delta;
    }

    public Context getTargetContext(String key) {
        // the thread to submit the operation may not be the thread to execute it.
        // we need to find the target context this thread is mapped to.
        int threadId =  Integer.parseInt(key) / delta;
        return tpg.threadToContextMap.get(threadId);
    }

    public Context getTargetContext(TableRecord d_record) {
        // the thread to submit the operation may not be the thread to execute it.
        // we need to find the target context this thread is mapped to.
        int threadId = getTaskId(d_record.record_.GetPrimaryKey(), delta);
        return tpg.threadToContextMap.get(threadId);
    }

    /**
     * Used by tpgScheduler.
     *
     * @param operation
     * @param mark_ID
     * @param clean
     */
    public void execute(Operation operation, long mark_ID, boolean clean) {
        // if the operation is in state aborted or committable or committed, we can bypass the execution
        if (operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED) || operation.isFailed) {
            //otherwise, skip (those already been tagged as aborted).
            commitLog(operation);
            return;
        }
        if (operation.accessType.equals(READ_WRITE_COND_READ)) {
            Transfer_Fun(operation, mark_ID, clean);
        } else if (operation.accessType.equals(READ_WRITE_COND)) {
            if (this.tpg.getApp() == 1) {//SL
                Transfer_Fun(operation, mark_ID, clean);
            }
        } else if (operation.accessType.equals(READ_WRITE)) {
            if (this.tpg.getApp() == 1) {
                Depo_Fun(operation, mark_ID, clean);
            }
        } else if (operation.accessType.equals(READ_WRITE_COND_READN)) {
            GrepSum_Fun(operation, mark_ID, clean);
        } else if (operation.accessType.equals(WRITE_ONLY)) {
            AppConfig.randomDelay();
            operation.d_record.record_.getValues().get(1).setLong(operation.value);
        } else if (operation.accessType.equals(READ_WRITE_READ)){
            assert operation.record_ref != null;
            if (this.tpg.getApp() == 2)
                TollProcess_Fun(operation, mark_ID, clean);
        } else {
            throw new UnsupportedOperationException();
        }
        commitLog(operation);
        assert operation.getOperationState() != MetaTypes.OperationStateType.EXECUTED;
    }

    // DD: Transfer event processing
    protected void Transfer_Fun(Operation operation, long previous_mark_ID, boolean clean) {
        MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(operation.context.thisThreadId);
        int success = operation.success[0];
        SchemaRecord preValues = operation.condition_records[0].content_.readPreValues(operation.bid);
        final long sourceAccountBalance = preValues.getValues().get(1).getLong();
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
                operation.success[0] ++;
            }
        }
        if (operation.record_ref != null) {
            operation.record_ref.setRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
        }
        if (operation.success[0] == success) {
            operation.isFailed = true;
        }
        MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(operation.context.thisThreadId);
        if (!operation.isFailed) {
            if (isLogging == LOGOption_path && !operation.pKey.equals(preValues.GetPrimaryKey()) && !operation.isCommit) {
                MeasureTools.BEGIN_SCHEDULE_TRACKING_TIME_MEASURE(operation.context.thisThreadId);
                int id = getTaskId(operation.pKey, delta);
                this.tpg.threadToPathRecord.get(id).addDependencyEdge(operation.table_name, operation.pKey, preValues.GetPrimaryKey(), operation.bid, sourceAccountBalance);
                operation.isCommit = true;
                MeasureTools.BEGIN_SCHEDULE_TRACKING_TIME_MEASURE(operation.context.thisThreadId);
            }
        }
    }

    protected void Depo_Fun(Operation operation, long mark_ID, boolean clean) {
        MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(operation.context.thisThreadId);
        SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
        List<DataBox> values = srcRecord.getValues();
        AppConfig.randomDelay();
        SchemaRecord tempo_record;
        tempo_record = new SchemaRecord(values);//tempo record
        tempo_record.getValues().get(1).incLong(operation.function.delta_long);//compute.
        operation.s_record.content_.updateMultiValues(operation.bid, mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
        MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(operation.context.thisThreadId);
    }

    protected void GrepSum_Fun(Operation operation, long previous_mark_ID, boolean clean) {
        MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(operation.context.thisThreadId);
        int success = operation.success[0];
        int keysLength = operation.condition_records.length;
        SchemaRecord[] preValues = new SchemaRecord[operation.condition_records.length];
        long sum = 0;
        AppConfig.randomDelay();
        for (int i = 0; i < keysLength; i++) {
            preValues[i] = operation.condition_records[i].content_.readPreValues(operation.bid);
            sum += preValues[i].getValues().get(1).getLong();
        }
        sum /= keysLength;
        if (operation.function.delta_long != -1) {
            // read
            SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record
            if (operation.function instanceof SUM) {
                tempo_record.getValues().get(1).setLong(sum);//compute.
            } else
                throw new UnsupportedOperationException();
            operation.d_record.content_.updateMultiValues(operation.bid, previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
            synchronized (operation.success) {
                operation.success[0]++;
            }
        }
        if (operation.record_ref != null) {
            operation.record_ref.setRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
        }
        if (operation.success[0] == success) {
            operation.isFailed = true;
        }
        MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(operation.context.thisThreadId);
        if (!operation.isFailed) {
            if (isLogging == LOGOption_path && !operation.isCommit) {
                for (int i = 0; i < keysLength; i++) {
                    if (!operation.pKey.equals(operation.condition_records[i].record_.GetPrimaryKey())) {
                        MeasureTools.BEGIN_SCHEDULE_TRACKING_TIME_MEASURE(operation.context.thisThreadId);
                        int id = getTaskId(operation.pKey, delta);
                        this.tpg.threadToPathRecord.get(id).addDependencyEdge(operation.table_name, operation.pKey, operation.condition_records[i].record_.GetPrimaryKey(), operation.bid, sum);
                        operation.isCommit = true;
                        MeasureTools.END_SCHEDULE_TRACKING_TIME_MEASURE(operation.context.thisThreadId);
                    }
                }
            }
        }
    }
    protected void TollProcess_Fun(Operation operation, long previous_mark_ID, boolean clean) {
        MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(operation.context.thisThreadId);
        int success = operation.success[0];
        AppConfig.randomDelay();
        List<DataBox> srcRecord = operation.s_record.record_.getValues();
        if (operation.function instanceof AVG) {
            if (operation.function.delta_double < MAX_SPEED) {
                double latestAvgSpeeds = srcRecord.get(1).getDouble();
                double lav;
                if (latestAvgSpeeds == 0) {//not initialized
                    lav = operation.function.delta_double;
                } else
                    lav = (latestAvgSpeeds + operation.function.delta_double) / 2;

                srcRecord.get(1).setDouble(lav);//write to state.
                operation.record_ref.setRecord(new SchemaRecord(new DoubleDataBox(lav)));//return updated record.
                synchronized (operation.success) {
                    operation.success[0] ++;
                }
            }
        } else {
            if (operation.function.delta_int < MAX_INT) {
                HashSet cnt_segment = srcRecord.get(1).getHashSet();
                cnt_segment.add(operation.function.delta_int);//update hashset; updated state also. TODO: be careful of this.
                operation.record_ref.setRecord(new SchemaRecord(new IntDataBox(cnt_segment.size())));//return updated record.
                synchronized (operation.success) {
                    operation.success[0] ++;
                }
            }
        }
        if (operation.success[0] == success) {
            operation.isFailed = true;
        }
        MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(operation.context.thisThreadId);
    }

    @Override
    public void AddContext(int threadId, Context context) {
        tpg.threadToContextMap.put(threadId, context);
        /*Thread to OCs does not need reconfigure*/
        tpg.setOCs(context);
    }


    /**
     * Submit requests to target thread --> data shuffling is involved.
     *
     * @param context
     * @param request
     * @return
     */
    @Override
    public boolean SubmitRequest(Context context, Request request) {
        context.push(request);
        return false;
    }

    protected abstract void DISTRIBUTE(Task task, Context context);

    @Override
    public void RESET(Context context) {
        //SOURCE_CONTROL.getInstance().oneThreadCompleted(context.thisThreadId);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        context.reset();
        tpg.reset(context);
    }

    @Override
    public void TxnSubmitFinished(Context context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        // the data structure to store all operations created from the txn, store them in order, which indicates the logical dependency
        int txnOpId = 0;
        Operation headerOperation = null;
        for (Request request : context.requests) {
            long bid = request.txn_context.getBID();
            Operation set_op;
            switch (request.accessType) {
                case WRITE_ONLY:
                    set_op = new Operation(request.src_key, getTargetContext(request.src_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, null, null, null, request.success);
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
                            request.d_record, request.record_ref, request.function, null, null, request.success);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
//            set_op.setConditionSources(request.condition_sourceTable, request.condition_source);
            tpg.setupOperationTDFD(set_op, request);
            if (txnOpId == 0)
                headerOperation = set_op;
            // addOperation an operation id for the operation for the purpose of temporal dependency construction
            set_op.setTxnOpId(txnOpId ++);
            set_op.addHeader(headerOperation);
            headerOperation.addDescendant(set_op);
        }
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
    }

    protected abstract void NOTIFY(Operation operation, Context context);

    public void start_evaluation(Context context, long mark_ID, int num_events) {
        int threadId = context.thisThreadId;

        INITIALIZE(context);

        do {
            EXPLORE(context);
            PROCESS(context, mark_ID);
        } while (!FINISHED(context));
        RESET(context);//
    }
    private void commitLog(Operation operation) {
        if (operation.isCommit)
            return;
        if (isLogging == LOGOption_path) {
            return;
        }
        MeasureTools.BEGIN_SCHEDULE_TRACKING_TIME_MEASURE(operation.context.thisThreadId);
        if (isLogging == LOGOption_wal) {
            ((LogRecord) operation.logRecord).addUpdate(operation.d_record.content_.readPreValues(operation.bid));
            this.loggingManager.addLogRecord(operation.logRecord);
        } else if (isLogging == LOGOption_dependency) {
            ((DependencyLog) operation.logRecord).setId(operation.bid + "." + operation.txnOpId);
            for (Operation op : operation.fd_parents) {
                ((DependencyLog) operation.logRecord).addInEdge(op.bid + "." + op.txnOpId);
            }
            for (Operation op : operation.td_parents) {
                ((DependencyLog) operation.logRecord).addInEdge(op.bid + "." + op.txnOpId);
            }
            for (Operation op : operation.fd_children) {
                ((DependencyLog) operation.logRecord).addOutEdge(op.bid + "." + op.txnOpId);
            }
            for (Operation op : operation.td_children) {
                ((DependencyLog) operation.logRecord).addOutEdge(op.bid + "." + op.txnOpId);
            }
            this.loggingManager.addLogRecord(operation.logRecord);
        } else if (isLogging == LOGOption_lv) {
            ((LVCLog) operation.logRecord).setAccessType(operation.accessType);
            ((LVCLog) operation.logRecord).setThreadId(operation.context.thisThreadId);
            this.loggingManager.addLogRecord(operation.logRecord);
        } else if (isLogging == LOGOption_command) {
            ((NativeCommandLog) operation.logRecord).setId(operation.bid + "." + operation.txnOpId);
            this.loggingManager.addLogRecord(operation.logRecord);
        }
        operation.isCommit = true;
        MeasureTools.END_SCHEDULE_TRACKING_TIME_MEASURE(operation.context.thisThreadId);
    }

}
