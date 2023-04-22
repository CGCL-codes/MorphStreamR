package common.param.lr;

import common.datatype.PositionReport;
import common.param.TxnEvent;
import storage.SchemaRecordRef;

/**
 * Currently only consider position events.
 */
public class LREvent extends TxnEvent {
    private final int tthread;
    private final long bid;
    private final PositionReport posreport;//input_event associated common.meta data.
    public int count;
    public double lav;
    public SchemaRecordRef speed_value;
    public SchemaRecordRef count_value;
    private long timestamp;

    /**
     * creating a new LREvent.
     *
     * @param posreport
     * @param tthread
     * @param bid
     */
    public LREvent(PositionReport posreport, int tthread, long bid) {
        super(bid);
        this.posreport = posreport;
        this.tthread = tthread;
        this.bid = bid;
        speed_value = new SchemaRecordRef();
        count_value = new SchemaRecordRef();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public PositionReport getPOSReport() {
        return posreport;
    }

    public int getPid() {
        return posreport.getSegment() % tthread;//which partition does this input_event belongs to.
    }

    public long getBid() {
        return bid;
    }

    @Override
    public LREvent cloneEvent() {
        LREvent lrEvent = new LREvent(this.posreport,tthread,bid);
        lrEvent.setTimestamp(timestamp);
        return lrEvent;
    }
    public String toString(){
        StringBuilder str = new StringBuilder();
        str.append(getBid());//0_bid
        str.append(",").append(getTimestamp());//1_timestamp
        str.append(",").append(posreport.getTime());//2_time
        str.append(",").append(posreport.getVid());//3_vid
        str.append(",").append(posreport.getSpeed());//4_speed
        str.append(",").append(posreport.getXWay());//5_xway
        str.append(",").append(posreport.getLane());//6_lane
        str.append(",").append(posreport.getDirection());//7_direction
        str.append(",").append(posreport.getSegment());//8_segment
        str.append(",").append(posreport.getPosition());//9_position
        return str.toString();
    }
}