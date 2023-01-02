package common.faulttolerance.inputReload;

import benchmark.DataHolder;
import common.collections.Configuration;
import common.param.sl.DepositEvent;
import common.param.sl.TransactionEvent;
import durability.inputStore.InputReload;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Queue;

import static common.CONTROL.enable_log;

public class SLInputReload extends InputReload {
    public SLInputReload(Configuration configuration) {
        this.tthread = configuration.getInt("tthread");
        this.partitionOffset = configuration.getInt("NUM_ITEMS") / tthread;
    }
    @Override
    public void reloadInput(BufferedReader reader, Queue<Object> lostEvents) throws IOException {
        String txn = reader.readLine();
        int[] p_bids = new int[tthread];
        while (txn != null) {
            String[] split = txn.split(",");
            int npid = (int) (Long.parseLong(split[1]) / partitionOffset);
            if (split.length == 8) {
                HashMap<Integer, Integer> pids = new HashMap<>();
                for (int i = 1; i < 5; i++) {
                    pids.put((int) (Long.parseLong(split[i]) / partitionOffset), 0);
                }
                TransactionEvent event = new TransactionEvent(
                        Integer.parseInt(split[0]), //bid
                        npid, //pid
                        Arrays.toString(p_bids), //bid_arrary
                        Arrays.toString(pids.keySet().toArray(new Integer[0])), // partition_index
                        4,//num_of_partition
                        split[1],//getSourceAccountId
                        split[2],//getSourceBookEntryId
                        split[3],//getTargetAccountId
                        split[4],//getTargetBookEntryId
                        Long.parseLong(split[5]), //getAccountTransfer
                        Long.parseLong(split[6])  //getBookEntryTransfer
                );
                event.setTimestamp(Long.parseLong(split[7]));
                lostEvents.add(event);
            } else if (split.length == 6) {
                HashMap<Integer, Integer> pids = new HashMap<>();
                for (int i = 1; i < 3; i++) {
                    pids.put((int) (Long.parseLong(split[i]) / partitionOffset), 0);
                }
                DepositEvent event = new DepositEvent(
                        Integer.parseInt(split[0]), //bid
                        npid, //pid
                        Arrays.toString(p_bids), //bid_array
                        Arrays.toString(pids.keySet().toArray(new Integer[0])), // partition_index
                        2,//num_of_partition
                        split[1],//getSourceAccountId
                        split[2],//getSourceBookEntryId
                        Long.parseLong(split[3]),  //getAccountDeposit
                        Long.parseLong(split[4])  //getBookEntryDeposit
                );
                event.setTimestamp(Long.parseLong(split[5]));
                lostEvents.add(event);
            }
            txn = reader.readLine();
        }
    }
}
