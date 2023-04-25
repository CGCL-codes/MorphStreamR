package common.faulttolerance.inputReload;

import common.collections.Configuration;
import common.collections.OsUtils;
import common.datatype.PositionReport;
import common.param.TxnEvent;
import common.param.lr.LREvent;
import common.tools.randomNumberGenerator;
import durability.inputStore.InputDurabilityHelper;
import profiler.MeasureTools;
import utils.FaultToleranceConstants;

import java.io.*;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

public class TPInputDurabilityHelper extends InputDurabilityHelper {
    public TPInputDurabilityHelper(Configuration configuration, int taskId, FaultToleranceConstants.CompressionType compressionType) {
        this.tthread = configuration.getInt("tthread");
        this.ftOption = configuration.getInt("FTOption");
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

    private void reloadInputWithCompression(File inputFile, Queue<Object> lostEvents, long redoOffset, long startOffset, int interval) {

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

    private void storeInputWithCompression(Object[] myevents, long currentOffset, int interval, File file) {

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
    private TxnEvent getEventFromString(String txn, long groupId) {
        String[] split = txn.split(",");
        long bid = Long.parseLong(split[0]);
        if (this.ftOption == 3 && historyViews.inspectAbortView(groupId, this.taskId, Integer.parseInt(split[0]))) {
            return null;
        }
        long timestamp = Long.parseLong(split[1]);
        PositionReport positionReport = new PositionReport((short) Integer.parseInt(split[2]),
                    Integer.parseInt(split[3]),
                    Integer.parseInt(split[4]),
                    Integer.parseInt(split[5]),
                    (short)Integer.parseInt(split[6]),
                    (short)Integer.parseInt(split[7]),
                    Integer.parseInt(split[8]),
                    Integer.parseInt(split[9]));
        LREvent lrEvent = new LREvent(positionReport, this.tthread, bid);
        lrEvent.setTimestamp(timestamp);
        return lrEvent;
    }
}
