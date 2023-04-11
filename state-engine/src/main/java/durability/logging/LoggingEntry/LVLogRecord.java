package durability.logging.LoggingEntry;

import content.common.CommonMetaTypes.AccessType;
import durability.struct.Logging.LVCLog;
import storage.TableRecord;

import java.util.concurrent.ConcurrentSkipListSet;

public class LVLogRecord {
    public int partitionId;
    public long allocatedLSN = 0;
    public ConcurrentSkipListSet<LVCLog> logs = new ConcurrentSkipListSet<>();
    public LVLogRecord(int partitionId) {
        this.partitionId = partitionId;
    }
    public void addLog(LVCLog log, TableRecord tableRecord, AccessType accessType) {
        //TODO: update the LVs according to the accessType
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
