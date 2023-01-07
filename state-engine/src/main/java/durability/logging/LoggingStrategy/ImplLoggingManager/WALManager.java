package durability.logging.LoggingStrategy.ImplLoggingManager;

import common.collections.Configuration;
import common.collections.OsUtils;
import common.io.ByteIO.DataInputView;
import common.io.ByteIO.InputWithDecompression.NativeDataInputView;
import common.tools.Deserialize;
import durability.ftmanager.AbstractRecoveryManager;
import durability.logging.LoggingResource.WalMetaInfoSnapshot;
import durability.logging.LoggingResult.Attachment;
import durability.logging.LoggingResult.LoggingHandler;
import durability.logging.LoggingStrategy.LoggingManager;
import durability.ftmanager.FTManager;
import durability.logging.LoggingEntry.LogRecord;
import durability.logging.LoggingResource.ImplLoggingResources.PartitionWalResources;
import durability.logging.LoggingStream.ImplLoggingStreamFactory.NIOWalStreamFactory;
import durability.recovery.RedoLogResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.SchemaRecord;
import storage.table.BaseTable;
import storage.table.RecordSchema;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;

import static java.nio.file.StandardOpenOption.READ;

public class WALManager implements LoggingManager {
    private static final Logger LOG = LoggerFactory.getLogger(WALManager.class);
    //<TableName, <PartitionId, LogRecords>>
    @Nonnull protected Map<String, Map<Integer, ConcurrentSkipListSet<LogRecord>>> pendingEntries;
    @Nonnull protected String walPath;
    protected int parallelNum;
    @Nonnull protected ConcurrentHashMap<String, WriteAheadLogTableInfo> metaInformation;
    protected final int num_items;
    protected final int delta;
    @Nonnull protected Map<String, BaseTable> tables;
    public WALManager(Map<String, BaseTable> tables, Configuration configuration) {
        this.tables = tables;
        parallelNum = configuration.getInt("parallelNum");
        walPath = configuration.getString("rootFilePath") + OsUtils.OS_wrapper("logging");
        num_items = configuration.getInt("NUM_ITEMS");
        delta = num_items / parallelNum;
        metaInformation = new ConcurrentHashMap<>();
        pendingEntries = new ConcurrentHashMap<>();
    }

    @Override
    public void registerTable(RecordSchema recordSchema, String tableName) {
        metaInformation.put(tableName, new WriteAheadLogTableInfo(tableName, recordSchema));
        ConcurrentHashMap<Integer, ConcurrentSkipListSet<LogRecord>> logs = new ConcurrentHashMap<>();
        for (int i = 0; i < parallelNum; i++) {
            logs.put(i, new ConcurrentSkipListSet<>());
        }
        pendingEntries.put(tableName, logs);
    }
    @Override
    public void addLogRecord(LogRecord logRecord) {
        this.pendingEntries.get(logRecord.tableName).get(getPartitionId(logRecord.key)).add(logRecord);
    }

    public PartitionWalResources syncPrepareResource(long groupId, int partitionId) {
        return new PartitionWalResources(groupId, partitionId, pendingEntries, metaInformation);
    }

    @Override
    public void commitLog(long groupId, int partitionId, FTManager ftManager) throws IOException {
        NIOWalStreamFactory nioWalStreamFactory = new NIOWalStreamFactory(this.walPath);
        PartitionWalResources partitionWalResources = syncPrepareResource(groupId, partitionId);
        AsynchronousFileChannel afc = nioWalStreamFactory.createLoggingStream();
        Attachment attachment = new Attachment(nioWalStreamFactory.getWalPath(), groupId, partitionId, afc, ftManager);
        ByteBuffer dataBuffer = partitionWalResources.createWriteBuffer();
        afc.write(dataBuffer, 0, attachment, new LoggingHandler());
    }

    @Override
    public void syncRedoWriteAheadLog(RedoLogResult redoLogResult) throws IOException {
        for (String path : redoLogResult.redoLogPaths) {
            Path walPath = Paths.get(path);
            AsynchronousFileChannel afc = AsynchronousFileChannel.open(walPath, READ);
            int fileSize = (int) afc.size();
            ByteBuffer dataBuffer = ByteBuffer.allocate(fileSize);
            Future<Integer> result = afc.read(dataBuffer, 0);
            //TODO:implementation compressionAlg, Different compressionAlg -> different dataInputView
            DataInputView inputView = new NativeDataInputView(dataBuffer);
            int walMetaInfoSize = inputView.readInt();
            WalMetaInfoSnapshot[] walMetaInfoSnapshots = new WalMetaInfoSnapshot[walMetaInfoSize];
            for (int i = 0; i < walMetaInfoSize; i ++) {
                byte[] object = inputView.readFullyDecompression();
                walMetaInfoSnapshots[i] = (WalMetaInfoSnapshot) Deserialize.Deserialize(object);
            }
            for (WalMetaInfoSnapshot metaInfo : walMetaInfoSnapshots) {
                int recordNum = metaInfo.logRecordNumber;
                while (recordNum != 0) {
                    byte[] objects = inputView.readFullyDecompression();
                    String logRecord = new String(objects, "UTF-8");
                    String[] values = logRecord.split(";");
                    int bid = Integer.parseInt(values[0]);
                    SchemaRecord schemaRecord = AbstractRecoveryManager.getRecord(metaInfo.recordSchema, values[1]);
                    this.tables.get(metaInfo.tableName).SelectKeyRecord(schemaRecord.GetPrimaryKey()).content_.updateMultiValues(bid, 0L, false, schemaRecord);
                    recordNum --;
                }
            }
        }
        LOG.info("Redo write-ahead log complete");
    }

    public static class WriteAheadLogTableInfo implements Serializable {
        public final String tableName;
        public final RecordSchema recordSchema;
        public WriteAheadLogTableInfo(String tableName, RecordSchema recordSchema) {
            this.recordSchema = recordSchema;
            this.tableName = tableName;
        }
    }

    public int getPartitionId(String primary_key) {
        int key = Integer.parseInt(primary_key);
        return key / delta;
    }

}
