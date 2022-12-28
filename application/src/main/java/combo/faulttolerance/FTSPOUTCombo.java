package combo.faulttolerance;

import benchmark.DataHolder;
import combo.SINKCombo;
import common.CONTROL;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.param.TxnEvent;
import components.context.TopologyContext;
import components.operators.api.TransactionalBolt;
import components.operators.api.TransactionalSpout;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Marker;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;
import utils.SOURCE_CONTROL;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.enable_log;
import static common.Constants.DEFAULT_STREAM_ID;
import static content.Content.CCOption_SStore;
import static content.Content.CCOption_TStream;
import static utils.FaultToleranceConstants.FTOption_ISC;
import static utils.FaultToleranceConstants.FTOption_WSC;

public abstract class FTSPOUTCombo extends TransactionalSpout implements FaultTolerance {
    private static Logger LOG;
    public final String split_exp = ";";
    public int the_end;
    public int global_cnt;
    public int num_events_per_thread;
    public long[] mybids;
    public Object[] myevents;
    public int counter;
    public Tuple tuple;
    public Tuple marker;
    public GeneralMsg generalMsg;
    public int tthread;
    public SINKCombo sink = new SINKCombo();
    protected int totalEventsPerBatch = 0;
    protected TransactionalBolt bolt;//compose the bolt here.
    int start_measure;
    Random random = new Random();

    public FTSPOUTCombo(Logger log, int i) {
        super(log, i);
        LOG = log;
        this.scalable = false;
    }

    public abstract void loadEvent(String file_name, Configuration config, TopologyContext context, OutputCollector collector) throws FileNotFoundException;

    @Override
    public void nextTuple() throws InterruptedException {
        try {
            if (counter == start_measure) {
                if (taskId == 0) {
                    sink.start();
                    DataHolder.SystemStartTime = System.nanoTime();
                }
            }
            if (counter < num_events_per_thread) {
                Object event = myevents[counter];

                long bid = mybids[counter];
                if (CONTROL.enable_latency_measurement){
                    long time;
                    if (arrivalControl) {
                        time = DataHolder.SystemStartTime + ((TxnEvent) event).getTimestamp();
                    } else {
                        time = System.nanoTime();
                    }
                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, time);
                } else {
                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
                }

                tuple = new Tuple(bid, this.taskId, context, generalMsg);
                bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
                counter++;

                if (ccOption == CCOption_TStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
                    if (model_switch(counter)) {
                        if (ftOption == FTOption_ISC && snapshot(counter)) {
                            marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "snapshot", counter));
                            if (this.taskId == 0) {
                                this.ftManager.spoutRegister(counter, "snapshot");
                            }
                        } else if (ftOption == FTOption_WSC){
                            if (snapshot(counter)) {
                                marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit_snapshot", counter));
                                if (this.taskId == 0) {
                                    this.ftManager.spoutRegister(counter, "snapshot");
                                }
                            } else {
                                marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit", counter));
                            }
                            if (this.taskId == 0) {
                                this.loggingManager.spoutRegister(counter, "commit");
                            }
                        } else {
                            marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration));
                        }
                        bolt.execute(marker);
                    }
                }
                if (counter == the_end) {
//                    if (ccOption == CCOption_SStore)
//                        MeasureTools.END_TOTAL_TIME_MEASURE(taskId);//otherwise deadlock.
                    SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
                    SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
                    if (taskId == 0)
                        sink.end(global_cnt);
                }
            }
        } catch (DatabaseException | BrokenBarrierException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer default_scale(Configuration conf) {
        return 1;//4 for 7 sockets
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        if (enable_log) LOG.info("Spout initialize is being called");
        long start = System.nanoTime();
        taskId = getContext().getThisTaskIndex();//context.getThisTaskId(); start from 0..
        long pid = OsUtils.getPID();
        if (enable_log) LOG.info("JVM PID  = " + pid);
        long end = System.nanoTime();
        if (enable_log) LOG.info("spout initialize takes (ms):" + (end - start) / 1E6);
        ccOption = config.getInt("CCOption", 0);
        ftOption = config.getInt("FTOption", 0);
        bid = 0;
        counter = 0;

        punctuation_interval = config.getInt("checkpoint");
        snapshot_interval = punctuation_interval * config.getInt("snapshotInterval");
        arrivalControl = config.getBoolean("arrivalControl");
        // setup the checkpoint interval for measurement
        sink.punctuation_interval = punctuation_interval;

        target_Hz = (int) config.getDouble("targetHz", 10000000);

        totalEventsPerBatch = config.getInt("totalEvents");
        tthread = config.getInt("tthread");

        num_events_per_thread = totalEventsPerBatch / tthread;

        if (enable_log) LOG.info("total events... " + totalEventsPerBatch);
        if (enable_log) LOG.info("total events per thread = " + num_events_per_thread);
        if (enable_log) LOG.info("checkpoint_interval = " + punctuation_interval);

        start_measure = CONTROL.MeasureStart;

        mybids = new long[num_events_per_thread];
        myevents = new Object[num_events_per_thread];
        the_end = num_events_per_thread;

        if (config.getInt("CCOption", 0) == CCOption_SStore) {
            global_cnt = (the_end) * tthread;
        } else {
            global_cnt = (the_end - CONTROL.MeasureStart) * tthread;
        }
    }
    @Override
    public boolean snapshot(int counter) throws InterruptedException, BrokenBarrierException {
        return (counter % snapshot_interval == 0);
    }
}
