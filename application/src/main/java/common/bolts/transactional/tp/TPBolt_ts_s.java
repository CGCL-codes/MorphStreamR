package common.bolts.transactional.tp;

import combo.SINKCombo;
import common.param.lr.LREvent;
import common.util.io.IOUtils;
import components.context.TopologyContext;
import db.DatabaseException;
import durability.logging.LoggingResult.LoggingResult;
import durability.snapshot.SnapshotResult.SnapshotResult;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import profiler.Metrics;
import transaction.context.TxnContext;
import transaction.function.AVG;
import transaction.function.CNT;
import transaction.function.Condition;
import transaction.impl.ordered.TxnManagerTStream;
import utils.FaultToleranceConstants;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.enable_latency_measurement;
import static profiler.MeasureTools.BEGIN_SNAPSHOT_TIME_MEASURE;
import static profiler.Metrics.NUM_ITEMS;

public class TPBolt_ts_s extends TPBolt {
    private static final Logger LOG= LoggerFactory.getLogger(TPBolt_ts_s.class);
    ArrayDeque<LREvent> LREvents;
    public TPBolt_ts_s(int fid,SINKCombo sink){
        super(LOG,fid,sink);
    }
    public TPBolt_ts_s(int fid){
        super(LOG,fid,null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager=new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(),thread_Id,
                NUM_ITEMS, this.context.getThisComponent().getNumTasks(),config.getString("scheduler","BF"));
        LREvents = new ArrayDeque<>();
    }
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID()
                , context.getGraph());
    }
    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException {
        if (in.isMarker()){
            int readSize = LREvents.size();
            {
                transactionManager.start_evaluate(thread_Id,in.getBID(),readSize);
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
                REQUEST_REQUEST_CORE();
            }
            if (Objects.equals(in.getMarker().getMessage(), "commit_early") || Objects.equals(in.getMarker().getMessage(), "commit_snapshot_early")) {
                this.loggingManager.boltRegister(this.thread_Id, FaultToleranceConstants.FaultToleranceStatus.Commit, new LoggingResult(in.getMarker().getSnapshotId(), this.thread_Id, null));
            }
            MeasureTools.BEGIN_POST_TIME_MEASURE(thread_Id);
            {
                REQUEST_POST();
            }
            MeasureTools.END_POST_TIME_MEASURE_ACC(thread_Id);
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
            LREvents.clear();
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id,readSize);
            if (this.sink.lastTask >= 0 && in.getMarker().getSnapshotId() * this.tthread >= this.sink.lastTask) {
                if (!this.sink.stopRecovery) {
                    this.sink.stopRecovery = true;
                    MeasureTools.END_RECOVERY_TIME_MEASURE(this.thread_Id);
                    MeasureTools.END_REPLAY_MEASURE(this.thread_Id);
                }
            }
            if (this.sink.stopRecovery) {
                Metrics.RecoveryPerformance.stopRecovery[thread_Id] = true;//Change here is to measure time for entire epoch.
                Metrics.RecoveryPerformance.recoveryItems[thread_Id] = this.sink.lastTask - this.sink.startRecovery;
                this.transactionManager.switch_scheduler(thread_Id, in.getBID());
            }
        } else {
            execute_ts_normal(in);
        }
    }

    @Override
    protected void PRE_TXN_PROCESS(long bid, long timestamp) throws DatabaseException, InterruptedException {
        MeasureTools.BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for(long i=_bid;i<_bid+_combo_bid_size;i++){
            TxnContext txnContext=new TxnContext(thread_Id,this.fid,i);
            LREvent event=(LREvent) input_event;
            if(enable_latency_measurement){
                (event).setTimestamp(timestamp);
            }
            REQUEST_CONSTRUCT(event,txnContext);
        }
    }
    protected void REQUEST_CONSTRUCT(LREvent event,TxnContext txnContext) throws DatabaseException{
        transactionManager.BeginTransaction(txnContext);
        transactionManager.Asy_ModifyRecord_Read(txnContext
                , "segment_speed"
                , String.valueOf(event.getPOSReport().getSegment())
                , event.speed_value
                , new AVG(event.getPOSReport().getSpeed())
                , event.success);
        transactionManager.Asy_ModifyRecord_Read(txnContext
                , "segment_cnt"
                , String.valueOf(event.getPOSReport().getSegment())
                , event.count_value//holder to be filled up.
                , new CNT(event.getPOSReport().getVid())
                , event.success);
        transactionManager.CommitTransaction(txnContext);
        LREvents.add(event);
    }
    protected void REQUEST_REQUEST_CORE() {
        for (LREvent event : LREvents) {
            TXN_REQUEST_CORE_TS(event);
        }
    }

    private void TXN_REQUEST_CORE_TS(LREvent event) {
        if (event.success[0] != 0){
            event.count = event.count_value.getRecord().getValue().getInt();
            event.lav = event.speed_value.getRecord().getValue().getDouble();
        }
    }
    protected void REQUEST_POST() throws InterruptedException {
        for (LREvent event : LREvents) {
            REQUEST_POST(event);
        }
    }
}
