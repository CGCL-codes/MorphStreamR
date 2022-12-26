package durability.logging.LoggingResource.ImplLoggingResources;

import common.io.ByteIO.DataOutputView;
import common.tools.Serialize;
import durability.logging.LoggingEntry.LogRecord;
import durability.logging.LoggingStrategy.ImplLoggingManager.WALManager;
import durability.logging.LoggingResource.WalMetaInfoSnapshot;
import durability.logging.LoggingResource.LoggingResources;
import scheduler.struct.MetaTypes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import static utils.FaultToleranceConstants.END_OF_TABLE_GROUP_MARK;

public class PartitionWalResources implements LoggingResources {
    private final List<WalMetaInfoSnapshot>  metaInfoSnapshots = new ArrayList<>();
    private final HashMap<String, ConcurrentSkipListSet<LogRecord>> logResources = new HashMap<>();
    private final long groupId;
    private final int partitionId;

    public PartitionWalResources(long groupId, int partitionId, Map<String, Map<Integer, ConcurrentSkipListSet<LogRecord>>> pendingEntries, ConcurrentHashMap<String, WALManager.WriteAheadLogTableInfo> metaInformation) {
        this.groupId = groupId;
        this.partitionId = partitionId;
        createLogMetaInfoSnapshots(metaInformation);
        createLogResources(pendingEntries);
    }
    private void createLogMetaInfoSnapshots(ConcurrentHashMap<String, WALManager.WriteAheadLogTableInfo> metaInformation) {
        for (WALManager.WriteAheadLogTableInfo info : metaInformation.values()) {
            this.metaInfoSnapshots.add(new WalMetaInfoSnapshot(info.tableName, this.partitionId));
        }
    }

    private void createLogResources(Map<String, Map<Integer, ConcurrentSkipListSet<LogRecord>>> pendingEntries) {
        for (Map.Entry<String, Map<Integer, ConcurrentSkipListSet<LogRecord>>> entry: pendingEntries.entrySet()) {
            this.logResources.put(entry.getKey(), entry.getValue().get(this.partitionId));
        }
    }
    public ByteBuffer createWriteBuffer() throws IOException {
        //TODO:implementation compressionAlg, Different compressionAlg -> different dataOutputView
        DataOutputView dataOutputView = new DataOutputView();
        writeLogMetaData(dataOutputView);
        writeLogRecord(dataOutputView);
        return ByteBuffer.wrap(dataOutputView.getByteArray());
    }

    private void writeLogMetaData(DataOutputView dataOutputView) throws IOException {
        dataOutputView.writeInt(this.metaInfoSnapshots.size());
        List<byte[]> objects = new ArrayList<>();
        for (WalMetaInfoSnapshot info : metaInfoSnapshots) {
            objects.add(Serialize.serializeObject(info));
        }
        for (byte[] o : objects) {
            dataOutputView.writeInt(o.length);
            dataOutputView.write(o);
        }
    }

    private void writeLogRecord(DataOutputView dataOutputView) throws IOException {
        Iterator<ConcurrentSkipListSet<LogRecord>> iterator = logResources.values().iterator();
        while (iterator.hasNext()) {
            dataOutputView.writeInt(END_OF_TABLE_GROUP_MARK);
            ConcurrentSkipListSet<LogRecord> records = iterator.next();
            Iterator<LogRecord> recordIterator = records.iterator();
            while(recordIterator.hasNext()) {
                LogRecord logRecord = recordIterator.next();
                if (logRecord.vote != MetaTypes.OperationStateType.ABORTED && logRecord.update != null) {
                    String str = logRecord.toString();
                    dataOutputView.writeInt(str.getBytes(StandardCharsets.UTF_8).length);
                    dataOutputView.write(str.getBytes(StandardCharsets.UTF_8));
                }
            }
        }
    }
}
