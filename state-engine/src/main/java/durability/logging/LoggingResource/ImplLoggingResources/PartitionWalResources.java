package durability.logging.LoggingResource.ImplLoggingResources;

import common.io.ByteIO.DataOutputView;
import common.io.ByteIO.OutputWithCompression.*;
import common.io.Compressor.RLECompressor;
import common.tools.Serialize;
import durability.logging.LoggingEntry.LogRecord;
import durability.logging.LoggingStrategy.ImplLoggingManager.WALManager;
import durability.logging.LoggingResource.WalMetaInfoSnapshot;
import durability.logging.LoggingResource.LoggingResources;
import durability.snapshot.LoggingOptions;
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
        createLogResources(pendingEntries);
        createLogMetaInfoSnapshots(metaInformation);
    }
    private void createLogMetaInfoSnapshots(ConcurrentHashMap<String, WALManager.WriteAheadLogTableInfo> metaInformation) {
        for (WALManager.WriteAheadLogTableInfo info : metaInformation.values()) {
            WalMetaInfoSnapshot walMetaInfoSnapshot = new WalMetaInfoSnapshot(info.recordSchema, info.tableName, this.partitionId);
            walMetaInfoSnapshot.setLogRecordNumber(logResources.get(info.tableName).size());
            this.metaInfoSnapshots.add(walMetaInfoSnapshot);
        }
    }

    private void createLogResources(Map<String, Map<Integer, ConcurrentSkipListSet<LogRecord>>> pendingEntries) {
        for (Map.Entry<String, Map<Integer, ConcurrentSkipListSet<LogRecord>>> entry: pendingEntries.entrySet()) {
            this.logResources.put(entry.getKey(), entry.getValue().get(this.partitionId));
        }
    }
    public ByteBuffer createWriteBuffer(LoggingOptions loggingOptions) throws IOException {
        DataOutputView dataOutputView;
        switch (loggingOptions.getCompressionAlg()) {
            case None:
                dataOutputView = new NativeDataOutputView();
                break;
            case Snappy:
                dataOutputView = new SnappyDataOutputView();
                break;
            case XOR:
                dataOutputView = new XORDataOutputView();
                break;
            case RLE:
                dataOutputView = new RLEDataOutputView();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + loggingOptions.getCompressionAlg());
        }
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
            dataOutputView.writeCompression(o);
        }
    }

    private void writeLogRecord(DataOutputView dataOutputView) throws IOException {
        for (WalMetaInfoSnapshot walMetaInfoSnapshot : metaInfoSnapshots) {
            ConcurrentSkipListSet<LogRecord> logRecords = logResources.get(walMetaInfoSnapshot.tableName);
            Iterator<LogRecord> recordIterator = logRecords.iterator();
            while(recordIterator.hasNext()) {
                LogRecord logRecord = recordIterator.next();
                if (logRecord.vote != MetaTypes.OperationStateType.ABORTED && logRecord.update != null) {
                    String str;
                    if (dataOutputView instanceof RLEDataOutputView) {
                        str = RLECompressor.encode(logRecord.toString());
                    } else {
                        str = logRecord.toString();
                    }
                    dataOutputView.writeCompression(str.getBytes(StandardCharsets.UTF_8));
                }
            }
            logRecords.clear();
        }
    }
}
