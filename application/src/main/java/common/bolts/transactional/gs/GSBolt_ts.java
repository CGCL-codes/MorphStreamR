package common.bolts.transactional.gs;

import combo.SINKCombo;
import common.param.mb.MicroEvent;
import common.util.io.IOUtils;
import db.DatabaseException;
import durability.logging.LoggingResult.LoggingResult;
import durability.snapshot.SnapshotResult.SnapshotResult;
import execution.ExecutionGraph;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import profiler.Metrics;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.function.SUM;
import transaction.impl.ordered.TxnManagerTStream;
import utils.FaultToleranceConstants;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.*;
import static profiler.MeasureTools.*;
import static profiler.Metrics.NUM_ITEMS;

public class GSBolt_ts extends GSBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GSBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    private final double write_useful_time = 556;//write-compute time pre-measured.
    Collection<MicroEvent> microEvents;
    private int writeEvents;

    public GSBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public GSBolt_ts(int fid) {
        super(LOG, fid, null);

    }

    /**
     * THIS IS ONLY USED BY TSTREAM.
     * IT CONSTRUCTS and POSTPONES TXNS.
     */
    protected void PRE_TXN_PROCESS(long _bid, long timestamp) throws DatabaseException, InterruptedException {
        BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            MicroEvent event = (MicroEvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            RANGE_WRITE_CONSRUCT((MicroEvent) event, txnContext);
        }

    }

    private void RANGE_WRITE_CONSRUCT(MicroEvent event, TxnContext txnContext) throws DatabaseException {
        SUM sum;
        if (event.READ_EVENT()) {
            sum = new SUM(-1);//It means this transaction should be aborted.
        } else
            sum = new SUM();

        transactionManager.BeginTransaction(txnContext);
        // multiple operations will be decomposed
        for (int i = 0; i < event.Txn_Length; i++) {
            int NUM_ACCESS = event.TOTAL_NUM_ACCESS / event.Txn_Length;
            String[] condition_table = new String[NUM_ACCESS];
            String[] condition_source = new String[NUM_ACCESS];
            for (int j = 0; j < NUM_ACCESS; j++) {
                int offset = i * NUM_ACCESS + j;
                condition_source[j] = String.valueOf(event.getKeys()[offset]);
                condition_table[j] = "MicroTable";
            }
            int writeKeyIdx = i * NUM_ACCESS;
            transactionManager.Asy_ModifyRecord_ReadN(
                    txnContext,
                    "MicroTable",
                    String.valueOf(event.getKeys()[writeKeyIdx]), // src key to write ahead
                    event.getRecord_refs()[writeKeyIdx],//to be fill up.
                    sum,
                    condition_table,
                    condition_source,//condition source, condition id.
                    event.success);          //asynchronously return.
        }
        transactionManager.CommitTransaction(txnContext);
        microEvents.add(event);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"));
        microEvents = new ArrayDeque<>();
    }

    void READ_REQUEST_CORE() throws InterruptedException {
        for (MicroEvent event : microEvents) {
            READ_CORE(event);
        }
    }

    void READ_POST() throws InterruptedException {
        for (MicroEvent event : microEvents) {
            READ_POST(event);
        }
    }
    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {

        if (in.isMarker()) {
            int num_events = microEvents.size();
            {
                {
                    transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
                    if (Objects.equals(in.getMarker().getMessage(), "snapshot")) {
                        MeasureTools.BEGIN_SNAPSHOT_TIME_MEASURE(this.thread_Id);
                        this.db.asyncSnapshot(in.getMarker().getSnapshotId(), this.thread_Id, this.ftManager);
                        MeasureTools.END_SNAPSHOT_TIME_MEASURE(this.thread_Id);
                    } else if (Objects.equals(in.getMarker().getMessage(), "commit") || Objects.equals(in.getMarker().getMessage(), "commit_early")) {
                        MeasureTools.BEGIN_LOGGING_TIME_MEASURE(this.thread_Id);
                        this.db.asyncCommit(in.getMarker().getSnapshotId(), this.thread_Id, this.loggingManager);
                        MeasureTools.END_LOGGING_TIME_MEASURE(this.thread_Id);
                    } else if (Objects.equals(in.getMarker().getMessage(), "commit_snapshot") || Objects.equals(in.getMarker().getMessage(), "commit_snapshot_early")){
                        MeasureTools.BEGIN_LOGGING_TIME_MEASURE(this.thread_Id);
                        this.db.asyncCommit(in.getMarker().getSnapshotId(), this.thread_Id, this.loggingManager);
                        MeasureTools.END_LOGGING_TIME_MEASURE(this.thread_Id);
                        BEGIN_SNAPSHOT_TIME_MEASURE(this.thread_Id);
                        this.db.asyncSnapshot(in.getMarker().getSnapshotId(), this.thread_Id, this.ftManager);
                        MeasureTools.END_SNAPSHOT_TIME_MEASURE(this.thread_Id);
                    }
                    READ_REQUEST_CORE();
                }
                if (Objects.equals(in.getMarker().getMessage(), "commit_early") || Objects.equals(in.getMarker().getMessage(), "commit_snapshot_early")) {
                    this.loggingManager.boltRegister(this.thread_Id, FaultToleranceConstants.FaultToleranceStatus.Commit, new LoggingResult(in.getMarker().getSnapshotId(), this.thread_Id, null));
                }
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    READ_POST();
                }
                END_POST_TIME_MEASURE_ACC(thread_Id);
                if (Objects.equals(in.getMarker().getMessage(), "snapshot")) {
                    this.ftManager.boltRegister(this.thread_Id, FaultToleranceConstants.FaultToleranceStatus.Commit, new SnapshotResult(in.getMarker().getSnapshotId(), this.thread_Id, null));
                } else if (Objects.equals(in.getMarker().getMessage(), "commit")){
                    this.loggingManager.boltRegister(this.thread_Id, FaultToleranceConstants.FaultToleranceStatus.Commit, new LoggingResult(in.getMarker().getSnapshotId(), this.thread_Id, null));
                } else if (Objects.equals(in.getMarker().getMessage(), "commit_snapshot")){
                    this.ftManager.boltRegister(this.thread_Id, FaultToleranceConstants.FaultToleranceStatus.Commit, new SnapshotResult(in.getMarker().getSnapshotId(), this.thread_Id, null));
                    this.loggingManager.boltRegister(this.thread_Id, FaultToleranceConstants.FaultToleranceStatus.Commit, new LoggingResult(in.getMarker().getSnapshotId(), this.thread_Id, null));
                } else if (Objects.equals(in.getMarker().getMessage(), "commit_snapshot_early")) {
                    this.ftManager.boltRegister(this.thread_Id, FaultToleranceConstants.FaultToleranceStatus.Commit, new SnapshotResult(in.getMarker().getSnapshotId(), this.thread_Id, null));
                }
                //all tuples in the holder is finished.
                microEvents.clear();
            }
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, num_events);
            if (this.sink.stopRecovery) {
                Metrics.RecoveryPerformance.stopRecovery[thread_Id] = true;//Change here is to measure time for entire epoch.
                Metrics.RecoveryPerformance.recoveryItems[thread_Id] = this.sink.lastTask - this.sink.startRecovery;
                this.transactionManager.switch_scheduler(thread_Id, in.getBID());
            }
        } else {
            execute_ts_normal(in);
        }
    }
}
