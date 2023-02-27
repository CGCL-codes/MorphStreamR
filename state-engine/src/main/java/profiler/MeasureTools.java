package profiler;

import common.CONTROL;
import common.collections.OsUtils;
import common.io.LocalFS.LocalDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static common.CONTROL.*;
import static common.IRunner.CCOption_MorphStream;
import static java.nio.file.StandardOpenOption.APPEND;
import static profiler.Metrics.*;
import static utils.FaultToleranceConstants.FTOption_ISC;
import static utils.FaultToleranceConstants.FTOption_WSC;

public class MeasureTools {
    private static final Logger log = LoggerFactory.getLogger(MeasureTools.class);
    public static AtomicInteger counter = new AtomicInteger(0);

    public static void Initialize() {
        Metrics.TxnRuntime.Initialize();
        Metrics.Runtime.Initialize();
        Metrics.Scheduler.Initialize();
        Metrics.Total_Record.Initialize();
        Metrics.Transaction_Record.Initialize();
        Metrics.Scheduler_Record.Initialize();
        Metrics.RuntimePerformance.Initialize();
        Metrics.RecoveryPerformance.Initialize();
    }

    public static void SCHEDULE_TIME_RECORD(int threadId, int num_events) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            RECORD_SCHEDULE_TIME(threadId, num_events);
    }

    public static void BEGIN_TOTAL_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            COMPUTE_START_TIME(thread_id);
            COMPUTE_PRE_EXE_START_TIME(thread_id);
        }
    }

    public static void END_TOTAL_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            RECORD_TIME(thread_id);
            RESET_COUNTERS(thread_id);
        }
    }

    public static void BEGIN_TOTAL_TIME_MEASURE_TS(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            if (Metrics.Runtime.Start[thread_id] == 0)
                COMPUTE_START_TIME(thread_id);
            COMPUTE_PRE_EXE_START_TIME(thread_id);
        }
    }

    public static void END_TOTAL_TIME_MEASURE_TS(int thread_id, int number_events) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            RECORD_TIME(thread_id, number_events);
            RESET_COUNTERS(thread_id);
        }
    }


    public static void END_PREPARE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            COMPUTE_PRE_EXE_TIME(thread_id);
        }
    }

    public static void END_PREPARE_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            COMPUTE_PRE_EXE_TIME_ACC(thread_id);
        }
    }

    public static void BEGIN_INDEX_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_START_INDEX_TIME(thread_id);
    }

    public static void END_INDEX_TIME_MEASURE_ACC(int thread_id, boolean is_retry_) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            COMPUTE_INDEX_TIME_ACC(thread_id);
        }
    }

    public static void BEGIN_POST_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_START_POST_EXE_TIME(thread_id);
    }

    public static void END_POST_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_POST_EXE_TIME(thread_id);
    }

    public static void END_POST_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_POST_EXE_TIME_ACC(thread_id);
    }

    public static void BEGIN_WAIT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_START_WAIT_TIME(thread_id);
    }

    public static void BEGIN_LOCK_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_START_LOCK_TIME(thread_id);
    }

    public static void END_LOCK_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_LOCK_TIME(thread_id);
    }

    public static void END_LOCK_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_LOCK_TIME_ACC(thread_id);
    }

    public static void END_WAIT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_WAIT_TIME(thread_id);
    }

    public static void END_WAIT_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_WAIT_TIME_ACC(thread_id);
    }

    public static void BEGIN_ABORT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_ABORT_START_TIME(thread_id);
    }

    public static void END_ABORT_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_ABORT_TIME_ACC(thread_id);
    }

    public static void BEGIN_ACCESS_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_START_ACCESS_TIME(thread_id);
    }

    //needs to include write compute time also for TS.
    public static void END_ACCESS_TIME_MEASURE_TS(int thread_id, int read_size,
                                                  double write_useful_time, int write_size) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            double write_time = write_useful_time * write_size;
            COMPUTE_ACCESS_TIME(thread_id, read_size, write_time);
        }
    }

    public static void END_ACCESS_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_ACCESS_TIME_ACC(thread_id);
    }

    public static void BEGIN_TXN_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_TXN_START_TIME(thread_id);
    }

    public static void END_TXN_TIME_MEASURE(int thread_id, int number_events) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            COMPUTE_TXN_TIME(thread_id);
            RECORD_TXN_BREAKDOWN_RATIO(thread_id, number_events);
        }
    }

    public static void END_TXN_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted()) {
            COMPUTE_TXN_TIME(thread_id);
            RECORD_TXN_BREAKDOWN_RATIO(thread_id);
        }
    }

    //TStream Specific.
    public static void BEGIN_PRE_TXN_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_PRE_TXN_START_TIME(thread_id);
    }

    public static void END_PRE_TXN_TIME_MEASURE_ACC(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_PRE_TXN_TIME_ACC(thread_id);
    }

    //Fault Tolerance Specific.
    public static void BEGIN_COMPRESSION_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_COMPRESSION_START_TIME(thread_id);
    }
    public static void END_COMPRESSION_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_COMPRESSION_TIME(thread_id);
    }
    public static void BEGIN_PERSIST_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_PERSIST_START_TIME(thread_id);
    }
    public static void END_PERSIST_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_PERSIST_TIME(thread_id);
    }
    public static void BEGIN_SNAPSHOT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SNAPSHOT_START_TIME(thread_id);
    }
    public static void END_SNAPSHOT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SNAPSHOT_TIME(thread_id);
    }
    public static void BEGIN_LOGGING_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_LOGGING_START_TIME(thread_id);
    }
    public static void END_LOGGING_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_LOGGING_TIME(thread_id);
    }



    // OGScheduler Specific.
    public static void BEGIN_SCHEDULE_NEXT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SCHEDULE_NEXT_START(thread_id);
    }

    public static void END_SCHEDULE_NEXT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SCHEDULE_NEXT_ACC(thread_id);
    }

    public static void BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SCHEDULE_EXPLORE_START(thread_id);
    }

    public static void END_SCHEDULE_EXPLORE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SCHEDULE_EXPLORE_ACC(thread_id);
    }

    public static void BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(int thread_id) {
        if (enable_debug) counter.incrementAndGet();
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted() && !Metrics.Scheduler.isAbort[thread_id])
            COMPUTE_SCHEDULE_USEFUL_START(thread_id);
    }

    public static void END_SCHEDULE_USEFUL_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted() && !Metrics.Scheduler.isAbort[thread_id])
            COMPUTE_SCHEDULE_USEFUL(thread_id);
    }

    public static void BEGIN_SCHEDULE_ABORT_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SCHEDULE_ABORT_START(thread_id);
    }

    public static void END_SCHEDULE_ABORT_TIME_MEASURE(int thread_id) {
        if (enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SCHEDULE_ABORT(thread_id);
    }

    public static void SCHEDULE_REDO_COUNT_MEASURE(int thread_id) {
        if (enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SCHEDULE_REDO_COUNT(thread_id);
    }

    public static void BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_CONSTRUCT_START(thread_id);
    }

    public static void END_TPG_CONSTRUCTION_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_CONSTRUCT(thread_id);
    }
    public static void BEGIN_SCHEDULER_SWITCH_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SWITCH_START(thread_id);
    }

    public static void END_SCHEDULER_SWITCH_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_SWITCH(thread_id);
    }

    public static void BEGIN_CACHE_OPERATION_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_CACHE_OPERATION_START(thread_id);
    }

    public static void END_CACHE_OPERATION_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_CACHE_OPERATION(thread_id);
    }

    public static void BEGIN_FIRST_EXPLORE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_FIRST_EXPLORE_START(thread_id);
    }

    public static void END_FIRST_EXPLORE_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_FIRST_EXPLORE(thread_id);
    }

    public static void BEGIN_NOTIFY_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_NOTIFY_START(thread_id);
    }

    public static void END_NOTIFY_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_NOTIFY(thread_id);
    }

    public static void THROUGHPUT_MEASURE(int thread_id, long count, double interval) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_THROUGHPUT(thread_id, count, interval);
    }
    public static void LATENCY_MEASURE(int thread_id, double latency) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            COMPUTE_LATENCY(thread_id, latency);
    }
    public static void setMetricDirectory(String directory) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            Metrics.directory = directory;
    }
    public static void setMetricFileNameSuffix(String suffix) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            fileNameSuffix = suffix;
    }
    // Fault Tolerance Specific.
    public static void setSnapshotSize(int thread_id, double size) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            RuntimePerformance.SnapshotSize[thread_id].addValue(size);
    }
    public static void setWriteAheadLog(int thread_id, double size) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            RuntimePerformance.WriteAheadLogSize[thread_id].addValue(size);
    }
    // Recovery Time Specific.
    public static void BEGIN_RECOVERY_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            RecoveryPerformance.COMPUTE_RECOVERY_START(thread_id);
    }
    public static void END_RECOVERY_TIME_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            RecoveryPerformance.COMPUTE_RECOVERY(thread_id);
    }
    public static void BEGIN_RELOAD_DATABASE_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            RecoveryPerformance.COMPUTE_RELOAD_DATABASE_START(thread_id);
    }
    public static void END_RELOAD_DATABASE_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            RecoveryPerformance.COMPUTE_RELOAD_DATABASE(thread_id);
    }
    public static void BEGIN_REDO_WAL_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            RecoveryPerformance.COMPUTE_REDO_START(thread_id);
    }
    public static void END_REDO_WAL_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            RecoveryPerformance.COMPUTE_REDO(thread_id);
    }
    public static void BEGIN_RELOAD_INPUT_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            RecoveryPerformance.COMPUTE_RELOAD_INPUT_START(thread_id);
    }
    public static void END_RELOAD_INPUT_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            RecoveryPerformance.COMPUTE_RELOAD_INPUT(thread_id);
    }
    public static void BEGIN_REPLAY_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            RecoveryPerformance.COMPUTE_REPLAY_START(thread_id);
    }
    public static void END_REPLAY_MEASURE(int thread_id) {
        if (CONTROL.enable_profile && !Thread.currentThread().isInterrupted())
            RecoveryPerformance.COMPUTE_REPLAY(thread_id);
    }

    private static void WriteThroughputReport(double throughput) {
        try {
            File file = new File(directory + fileNameSuffix + ".overall");
            file.mkdirs();
            if (file.exists()) {
                try {
                    file.delete();
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("Throughput: " + throughput + "\n");
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void AverageTotalTimeBreakdownReport(int tthread, int snapshotInterval) {
        try {
            File file = new File(directory + fileNameSuffix + ".overall");
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("AverageTotalTimeBreakdownReport\n");
            if (enable_log) log.info("===Average Total Time Breakdown Report===");
            fileWriter.write("total_time\t serialize_time\t persist_time\t stream_process\t txn_process\t overheads\n");
            if (enable_log) log.info("total_time\t serialize_time\t persist_time\t stream_process\t txn_process\t overheads");
            double totalProcessTime = 0;
            double totalSerializeTime = 0;
            double totalPersistTime = 0;
            double totalStreamProcessTime = 0;
            double totalTxnProcessTime = 0;
            double totalOverheads = 0;
            for (int threadId = 0; threadId < tthread; threadId++) {
                totalProcessTime += Total_Record.totalProcessTimePerEvent[threadId].getMean();
                if (Total_Record.compression_total[threadId].getN() > 0) {
                    totalSerializeTime += Total_Record.compression_total[threadId].getMean() + Total_Record.serialization_total[threadId].getMean() + Total_Record.snapshot_serialization_total[threadId].getMean() / snapshotInterval;
                }
                totalPersistTime += Total_Record.persist_total[threadId].getMean();
                totalStreamProcessTime += Total_Record.stream_total[threadId].getMean();
                totalTxnProcessTime += Total_Record.txn_total[threadId].getMean();
                totalOverheads += Total_Record.overhead_total[threadId].getMean();
            }
            String output = String.format(
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f"
                    , totalProcessTime / tthread
                    , totalSerializeTime / tthread
                    , totalPersistTime / tthread
                    , totalStreamProcessTime / tthread
                    , totalTxnProcessTime / tthread
                    , totalOverheads / tthread
            );
            fileWriter.write(output + "\n");
            if (enable_log) log.info(output);
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void WriteThroughputPerPhase(int tthread, int phase, int shiftRate) {
        try {
            double[] tr = new double[Metrics.Runtime.ThroughputPerPhase.get(0).size()];
            //every punctuation
            for (int i = 0; i < tr.length; i++) {
                tr[i] = 0;
                for (int j = 0; j < tthread; j++){
                    tr[i] = tr[i] + Metrics.Runtime.ThroughputPerPhase.get(j).get(i) * 1E6;//
                }
            }
            //every phase
            double[] tr_p = new double[tr.length / shiftRate];
            for (int i = 0; i < tr_p.length; i++) {
                tr_p[i] = 0;
                for (int j = i * shiftRate; j < (i + 1) * shiftRate; j++) {
                    tr_p[i] = tr_p[i] + tr[j];
                }
                tr_p[i] = tr_p[i] / shiftRate;
            }
            StringBuilder stringBuilder = new StringBuilder();
            File file = new File(directory + fileNameSuffix + ".overall");
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("phase_id\t throughput\n");
            for (int i = 0; i < tr_p.length; i++){
                String output = String.format("%d\t" +
                                "%-10.4f\t"
                        , i,tr_p[i]
                );
                stringBuilder.append(output);
                fileWriter.write(output + "\n");
            }
            fileWriter.close();
            if (enable_log) log.info(String.valueOf(stringBuilder));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void WritePersistFileSize(int ftOption, int tthread) {
        try {
            File file = new File(directory + fileNameSuffix + ".overall");
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            if (ftOption == FTOption_ISC || ftOption == FTOption_WSC) {
                fileWriter.write("SnapshotSizeReport (KB): " + "\n");
                fileWriter.write("thread_id" + "\t" + "size" + "\n");
                double totalSize = 0;
                for (int i = 0; i < tthread; i ++) {
                    totalSize = totalSize + RuntimePerformance.SnapshotSize[i].getMean();
                    fileWriter.write(i + "\t" + RuntimePerformance.SnapshotSize[i].getMean() + "\n");
                }
                fileWriter.write("SnapshotTotalSize (KB): " + totalSize + "\n");
            }
            if (ftOption == FTOption_WSC) {
                fileWriter.write("WriteAheadLogSize: " + "\n");
                fileWriter.write("thread_id" + "\t" + "size (KB)" + "\n");
                double totalSize = 0;
                for (int i = 0; i < tthread; i ++) {
                    totalSize = totalSize + RuntimePerformance.WriteAheadLogSize[i].getMean();
                    fileWriter.write(i + "\t"+ RuntimePerformance.WriteAheadLogSize[i].getMean() + "\n");
                }
                fileWriter.write("WriteAheadLogTotalSize (KB): " + totalSize + "\n");
            }
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void WriteRecoveryTime(int tthread) {
        try {
            File file = new File(directory + fileNameSuffix + ".overall");
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("RecoveryTimeReport (ms): " + "\n");
            fileWriter.write("thread_id" + "\t" + "time" + "\n");
            for (int i = 0; i < tthread; i++) {
                fileWriter.write(i + "\t" + RecoveryPerformance.RecoveryTime[i].getMean() + "\n");
            }
            fileWriter.close();
            WriteRecoveryTimeBreakDown(tthread);
            WriteReplayTimeBreakDown(tthread);
            WriteRecoverySchedulerTimeBreakdownReport(tthread);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void WriteRecoveryTimeBreakDown(int tthread) {
        try {
            File file = new File(directory + fileNameSuffix + ".overall");
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("RecoveryTimeBreakDownReport (ms): " + "\n");
            fileWriter.write("thread_id\t reloadDatabaseTime\t redoWriteAheadLogTime\t reloadInputTime\t replayTime\n");
            for (int threadId = 0; threadId < tthread; threadId++) {
                String output = String.format("%d\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , threadId
                        , RecoveryPerformance.ReloadDatabaseTime[threadId].getMean()
                        , RecoveryPerformance.RedoWriteAheadLogTime[threadId].getMean()
                        , RecoveryPerformance.ReloadInputTime[threadId].getMean()
                        , RecoveryPerformance.ReplayTime[threadId].getMean()
                );
                fileWriter.write(output + "\n");
            }
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void WriteReplayTimeBreakDown(int tthread) {
        try {
            File file = new File(directory + fileNameSuffix + ".overall");
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("ReplayTimeBreakDownReport (ms): " + "\n");
            fileWriter.write("thread_id\t total_time\t stream_process\t txn_process\t overheads\n");
            for (int threadId = 0; threadId < tthread; threadId++) {
                String output = String.format("%d\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , threadId
                        , RecoveryPerformance.total_time[threadId] / 1E6
                        , RecoveryPerformance.stream_total[threadId] / 1E6
                        , RecoveryPerformance.txn_total[threadId] / 1E6
                        , RecoveryPerformance.overhead_total[threadId] / 1E6
                );
                fileWriter.write(output + "\n");
            }
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void WriteRecoverySchedulerTimeBreakdownReport(int tthread) {
        try {
            File file = new File(directory + fileNameSuffix + ".overall");
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("RecoverySchedulerTimeBreakdownReport (ms): " + "\n");
            fileWriter.write("thread_id\t explore_time\t next_time\t useful_time\t abort_time\t notify_time\t construct_time\t first_explore_time\t scheduler_switch\n");
            for (int threadId = 0; threadId < tthread; threadId++) {
                String output = String.format("%d\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , threadId
                        , RecoveryPerformance.Explore[threadId] / 1E6
                        , RecoveryPerformance.Next[threadId] / 1E6
                        , RecoveryPerformance.Useful[threadId] / 1E6
                        , RecoveryPerformance.Abort[threadId] / 1E6
                        , RecoveryPerformance.Notify[threadId] / 1E6
                        , RecoveryPerformance.Construct[threadId] / 1E6
                        , RecoveryPerformance.FirstExplore[threadId] / 1E6
                        , RecoveryPerformance.SchedulerSwitch[threadId] / 1E6
                );
                fileWriter.write(output + "\n");
            }
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private static void SchedulerTimeBreakdownReport(int tthread) {
        try {
            if (enable_debug) log.info("++++++ counter: " + counter);
            File file = new File(directory + fileNameSuffix + ".overall");
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("SchedulerTimeBreakdownReport (ns)\n");
            if (enable_log) log.info("===Scheduler Time Breakdown Report===");
            fileWriter.write("explore_time\t useful_time\t abort_time\t construct_time\t first_explore_time\t next_time\t notify_time\t scheduler_switch\n");
            if (enable_log)
                log.info("explore_time\t useful_time\t abort_time\t construct_time\t first_explore_time\t next_time\t notify_time\t scheduler_switch");
            double explore_time = 0;
            double useful_time = 0;
            double abort_time = 0;
            double construct_time = 0;
            double first_explore_time = 0;
            double next_time = 0;
            double notify_time = 0;
            double scheduler_switch = 0;
            for (int threadId = 0; threadId < tthread; threadId++) {
                explore_time += Scheduler_Record.Explore[threadId].getMean();
                useful_time += Scheduler_Record.Useful[threadId].getMean();
                abort_time += Scheduler_Record.Abort[threadId].getMean();
                construct_time += Scheduler_Record.Construct[threadId].getMean();
                first_explore_time += Scheduler_Record.FirstExplore[threadId].getMean();
                next_time += Scheduler_Record.Next[threadId].getMean();
                notify_time += Scheduler_Record.Notify[threadId].getMean();
                scheduler_switch += Scheduler_Record.SchedulerSwitch[threadId].getMean();
            }
            String output = String.format(
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t" +
                            "%-10.2f\t"
                    , explore_time / tthread
                    , useful_time / tthread
                    , abort_time / tthread
                    , construct_time / tthread
                    , first_explore_time / tthread
                    , next_time / tthread
                    , notify_time / tthread
                    , scheduler_switch / tthread
            );
            if (enable_log) log.info(output);
            fileWriter.write(output + "\n");
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private static void TransactionBreakdownRatioReport(int ccOption, int tthread) {
        try {
            File file = new File(directory + fileNameSuffix + ".overall");
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            fileWriter.write("TransactionBreakdownRatioReport (ns)\n");
            if (enable_log) log.info("===TXN BREAKDOWN===");
            fileWriter.write("thread_id\t index_ratio\t useful_ratio\t sync_ratio\t lock_ratio\n");
            if (enable_log) log.info("thread_id\t index_ratio\t useful_ratio\t sync_ratio\t lock_ratio");
            for (int threadId = 0; threadId < tthread; threadId++) {
                String output = String.format("%d\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t" +
                                "%-10.2f\t"
                        , threadId
                        , Transaction_Record.index_ratio[threadId].getMean()
                        , Transaction_Record.useful_ratio[threadId].getMean()
                        , Transaction_Record.sync_ratio[threadId].getMean()
                        , ccOption == CCOption_MorphStream ? 0 : Transaction_Record.lock_ratio[threadId].getMean()
                );
                fileWriter.write(output + "\n");
                if (enable_log) log.info(output);
            }
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void WriteRuntimePerformance(int tthread) {
        try {
            File file = new File(directory + fileNameSuffix + ".runtime");
            file.mkdirs();
            if (file.exists()) {
                try {
                    file.delete();
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()));
            fileWriter.write("throughput\t latency\n");
            List<double[]> latencys = new ArrayList<>();
            List<double[]> throughputs = new ArrayList<>();
            for (int i = 0; i < tthread; i++) {
                latencys.add(RuntimePerformance.Latency[i].getValues());
                throughputs.add(RuntimePerformance.Throughput[i].getValues());
            }
            loop: for (int i = 0; i < latencys.get(0).length; i++) {
                double throughput = 0;
                double latency = 0;
                for (int j = 0; j < tthread; j++) {
                    if (i >= throughputs.get(j).length || i >= latencys.get(j).length) {
                        break loop;
                    }
                    throughput = throughput + throughputs.get(j)[i];
                    latency = latency + latencys.get(j)[i];
                }
                fileWriter.write(throughput + "\t" + latency / tthread + "\n");
            }
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void WriteLastTasks(String rootFile, int tthread) {
        try {
            for (int i = 0; i < tthread; i ++) {
                File file = new File(rootFile + OsUtils.OS_wrapper("outputStore") + OsUtils.OS_wrapper(i + ".output"));
                file.mkdirs();
                if (file.exists()) {
                    try {
                        file.delete();
                        file.createNewFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                LocalDataOutputStream outputStream = new LocalDataOutputStream(file);
                DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
                dataOutputStream.writeLong(RuntimePerformance.lastTasks[i]);
                dataOutputStream.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void WriteMemoryConsumption() {
        if (enable_memory_measurement) {
            timer.cancel();
            try {
                File file = new File(directory + fileNameSuffix + ".memory");
                file.mkdirs();
                if (file.exists()) {
                    try {
                        file.delete();
                        file.createNewFile();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                BufferedWriter w = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
                w.write("UsedMemory\n");
                for (int i = 0; i < usedMemory.getValues().length; i ++){
                    String output = String.format("%f\t" +
                                    "%-10.4f\t"
                            , (float) i ,usedMemory.getValues()[i]);
                    w.write(output + "\n");
                }
                w.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public static void WriteSSDConsumption() {
        try {
            File file = new File(directory + fileNameSuffix + ".ssd");
            file.mkdirs();
            if (file.exists()) {
                try {
                    file.delete();
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            BufferedWriter w = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            w.write("IncreaseFileSize (KB):\n");
            for (int i = 0; i < usedFileSize.getValues().length; i ++){
                String output = String.format("%f\t" +
                                "%-10.4f\t"
                        , (float) i ,usedFileSize.getValues()[i]);
                w.write(output + "\n");
            }
            w.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void WriteSSDBandwidth() {
        try {
            File file = new File(directory + fileNameSuffix + ".bandwidth");
            file.mkdirs();
            if (file.exists()) {
                try {
                    file.delete();
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            BufferedWriter w = Files.newBufferedWriter(Paths.get(file.getPath()), APPEND);
            w.write("Bandwidth (kb/s):\n");
            for (int i = 0; i < SSDBandwidth.getValues().length; i ++){
                String output = String.format("%f\t" +
                                "%-10.4f\t"
                        , (float) i ,SSDBandwidth.getValues()[i]);
                w.write(output + "\n");
            }
            w.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void METRICS_REPORT(int ccOption, int FTOption, int tthread, double throughput, int phase, int shiftRate, int snapshotInterval) {
        WriteThroughputReport(throughput);
        AverageTotalTimeBreakdownReport(tthread, snapshotInterval);
        WritePersistFileSize(FTOption, tthread);
        //WriteThroughputPerPhase(tthread, phase, shiftRate);
        if (fileNameSuffix.equals("_recovery")) {
            WriteRecoveryTime(tthread);
        }
        if (ccOption == CCOption_MorphStream) {//extra info
            SchedulerTimeBreakdownReport(tthread);
        } else {
            TransactionBreakdownRatioReport(ccOption, tthread);
        }
        WriteRuntimePerformance(tthread);
        WriteMemoryConsumption();
        WriteSSDConsumption();
        WriteSSDBandwidth();
    }
    public static void METRICS_REPORT_WITH_FAILURE(int ccOption, int FTOption, int tthread, String rootFile, int snapshotInterval) {
        File file = new File(directory + fileNameSuffix + ".overall");
        file.mkdirs();
        if (file.exists()) {
            try {
                file.delete();
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        AverageTotalTimeBreakdownReport(tthread, snapshotInterval);
        WritePersistFileSize(FTOption, tthread);
        WriteRuntimePerformance(tthread);
        if (ccOption == CCOption_MorphStream) {//extra info
            SchedulerTimeBreakdownReport(tthread);
        } else {
            TransactionBreakdownRatioReport(ccOption, tthread);
        }
        WriteRuntimePerformance(tthread);
        WriteMemoryConsumption();
        WriteSSDConsumption();
        WriteSSDBandwidth();
        WriteLastTasks(rootFile, tthread);
    }
}