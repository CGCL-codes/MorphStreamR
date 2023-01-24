package durability.snapshot.SnapshotResources.ImplSnapshotResources;

import common.io.ByteIO.DataOutputView;
import common.io.ByteIO.OutputWithCompression.*;
import common.io.Compressor.RLECompressor;
import common.tools.Serialize;
import durability.snapshot.SnapshotOptions;
import durability.snapshot.SnapshotResources.SnapshotResources;
import durability.snapshot.SnapshotResources.StateMetaInfoSnapshot;
import durability.snapshot.SnapshotStrategy.ImplSnapshotStrategy.InMemorySnapshotStrategy;
import storage.TableRecord;
import storage.table.BaseTable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static utils.FaultToleranceConstants.END_OF_TABLE_GROUP_MARK;

public class InMemoryFullSnapshotResources implements SnapshotResources {
    private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = new ArrayList<>();
    //<TableName, <Key, TableRecord>>
    private final HashMap<String, HashMap<String, TableRecord>> snapshotResource = new HashMap<>();
    private long snapshotId;
    private int partitionId;

    public InMemoryFullSnapshotResources(long snapshotId, int partitionId, Map<String, InMemorySnapshotStrategy.InMemoryKvStateInfo> kvStateInformation, Map<String, BaseTable> tables) {
        this.snapshotId = snapshotId;
        this.partitionId = partitionId;
        createSnapshotResources(tables);
        createStateMetaInfoSnapshot(kvStateInformation);
    }

    private void createStateMetaInfoSnapshot(Map<String, InMemorySnapshotStrategy.InMemoryKvStateInfo> kvStateInformation) {
        for (InMemorySnapshotStrategy.InMemoryKvStateInfo info : kvStateInformation.values()) {
            StateMetaInfoSnapshot stateMetaInfoSnapshot = new StateMetaInfoSnapshot(info.recordSchema, info.tableName, this.partitionId);
            stateMetaInfoSnapshot.setRecordNum(snapshotResource.get(info.tableName).size());
            this.stateMetaInfoSnapshots.add(stateMetaInfoSnapshot);
        }
    }
    private void createSnapshotResources(Map<String, BaseTable> tables) {
        for (Map.Entry<String, BaseTable> table:tables.entrySet()) {
            snapshotResource.put(table.getKey(), table.getValue().getTableIndexByPartitionId(this.partitionId));
        }
    }

    public ByteBuffer createWriteBuffer(SnapshotOptions snapshotOptions) throws IOException {
        DataOutputView dataOutputView;
        switch (snapshotOptions.getCompressionAlg()) {
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
                throw new IllegalStateException("Unexpected value: " + snapshotOptions.getCompressionAlg());
        }
        writeKVStateMetaData(dataOutputView);
        writeKVStateDate(dataOutputView);
        return ByteBuffer.wrap(dataOutputView.getByteArray());
    }

    private void writeKVStateMetaData(DataOutputView dataOutputView) throws IOException {
        dataOutputView.writeInt(this.stateMetaInfoSnapshots.size());
        List<byte[]> objects = new ArrayList<>();
        for (StateMetaInfoSnapshot stateMetaInfoSnapshot : this.stateMetaInfoSnapshots) {
            objects.add(Serialize.serializeObject(stateMetaInfoSnapshot));
        }
        for (byte[] o: objects) {
            dataOutputView.writeCompression(o);
        }
    }

    private void writeKVStateDate(DataOutputView dataOutputView) throws IOException {
        for (StateMetaInfoSnapshot stateMetaInfoSnapshot : stateMetaInfoSnapshots) {
            HashMap<String, TableRecord> tables = snapshotResource.get(stateMetaInfoSnapshot.tableName);
            Iterator<TableRecord> recordIterator = tables.values().iterator();
            while (recordIterator.hasNext()) {
                TableRecord tableRecord = recordIterator.next();
                String str;
                if (dataOutputView instanceof RLEDataOutputView) {
                    str = RLECompressor.encode(tableRecord.toSerializableString(this.snapshotId));
                } else {
                    str = tableRecord.toSerializableString(this.snapshotId);
                }
                dataOutputView.writeCompression(str.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    @Override
    public void release() {

    }
}
