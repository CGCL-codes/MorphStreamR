package common.faulttolerance.inputReload;

import common.collections.Configuration;
import common.collections.OsUtils;
import common.param.TxnEvent;
import common.param.mb.MicroEvent;
import durability.inputStore.InputDurabilityHelper;
import profiler.MeasureTools;
import utils.FaultToleranceConstants;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

public class GSInputDurabilityHelp extends InputDurabilityHelper {
    public GSInputDurabilityHelp(Configuration configuration, int taskId, FaultToleranceConstants.CompressionType compressionType) {
        this.tthread = configuration.getInt("tthread");
        this.partitionOffset = configuration.getInt("NUM_ITEMS") / tthread;
        this.encodingType = compressionType;
        this.taskId = taskId;
        switch(compressionType) {
            case None:
                this.isCompression = false;
                break;
        }
    }

    @Override
    public void reloadInput(File inputFile, Queue<Object> lostEvents, long redoOffset, long startOffset, int interval) throws IOException, ExecutionException, InterruptedException {
        if (isCompression) {
            reloadInputWithCompression(inputFile, lostEvents, redoOffset, startOffset, interval);
        } else {
            reloadInputWithoutCompression(inputFile, lostEvents, redoOffset, startOffset, interval);
        }
    }

    private void reloadInputWithoutCompression(File inputFile, Queue<Object> lostEvents, long redoOffset, long startOffset, int interval) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile)));
        while(startOffset != redoOffset) {
            reader.readLine();
            startOffset ++;
        }
        int number = 0;
        long groupId = startOffset + interval;
        String txn = reader.readLine();
        while (txn != null) {
            TxnEvent event = getEventFromString(txn, groupId);
            if (event != null) {
                lostEvents.add(event);
            }
            number ++;
            if (number == interval) {
                groupId += interval;
                number = 0;
            }
            txn = reader.readLine();
        }
        reader.close();
    }

    private void reloadInputWithCompression(File inputFile, Queue<Object> lostEvents, long redoOffset, long startOffset, int interval) {

    }

    @Override
    public void storeInput(Object[] myevents, long currentOffset, int interval, String inputStoreCurrentPath) throws IOException, ExecutionException, InterruptedException {
        File file = new File(inputStoreCurrentPath);
        if (!file.exists()) {
            file.mkdirs();
        }
        file = new File(inputStoreCurrentPath + OsUtils.OS_wrapper(taskId + ".input"));
        if (!file.exists())
            file.createNewFile();
        if (!isCompression) {
            storeInputWithoutCompression(myevents, currentOffset, interval, file);
        } else {
            storeInputWithCompression(myevents, currentOffset, interval, file);
        }
    }
    private void storeInputWithoutCompression(Object[] myevents, long currentOffset, int interval, File inputFile) throws IOException {
        MeasureTools.BEGIN_PERSIST_TIME_MEASURE(this.taskId);
        BufferedWriter EventBufferedWriter = new BufferedWriter(new FileWriter(inputFile, true));
        for (int i = (int) currentOffset; i < currentOffset + interval; i ++) {
            EventBufferedWriter.write( myevents[i].toString() + "\n");
        }
        EventBufferedWriter.close();
        MeasureTools.END_PERSIST_TIME_MEASURE(this.taskId);
    }
    private void storeInputWithCompression(Object[] myevents, long currentOffset, int interval, File file) {

    }
    private TxnEvent getEventFromString(String txn, long groupId) {
        int[] p_bids = new int[tthread];
        String[] split = txn.split(",");
        if (historyViews.inspectAbortView(groupId, this.taskId, Integer.parseInt(split[0]))) {
            return null;
        }
        int npid = (int) (Long.parseLong(split[1]) / partitionOffset);
        int keyLength  = split.length - 4;
        long[] keys = new long[keyLength];
        HashMap<Integer, Integer> pids = new HashMap<>();
        for (int i = 1; i < keyLength + 1; i++) {
            keys[i-1] = Long.parseLong(split[i]);
            pids.put((int) (keys[i-1] / partitionOffset), 0);
        }
        MicroEvent event = new MicroEvent(
                Integer.parseInt(split[0]), //bid,
                npid, //pid
                Arrays.toString(p_bids), //bid_array
                Arrays.toString(pids.keySet().toArray(new Integer[0])), // partition_index
                pids.size(), // num_of_partition
                Arrays.toString(keys), // key_array
                keyLength,// num_of_key
                Integer.parseInt(split[keyLength + 1]),//Transaction length
                Boolean.parseBoolean(split[keyLength + 2]));//Is abort
        event.setTimestamp(Long.parseLong(split[keyLength + 3]));
        return event;
    }


}
