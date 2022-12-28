package benchmark;

import common.param.TxnEvent;

import java.util.ArrayList;

public class DataHolder {
    public static ArrayList<TxnEvent> events = new ArrayList<>();
    public static long EventStartTime;
    public static long SystemStartTime;
}