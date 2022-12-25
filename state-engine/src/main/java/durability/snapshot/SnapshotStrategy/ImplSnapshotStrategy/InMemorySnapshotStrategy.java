package durability.snapshot.SnapshotStrategy.ImplSnapshotStrategy;

import durability.ftmanager.FTManager;
import durability.snapshot.SnapshotOptions;
import durability.snapshot.SnapshotResources.ImplSnapshotResources.InMemoryFullSnapshotResources;
import durability.snapshot.SnapshotResult.Attachment;
import durability.snapshot.SnapshotResult.SnapshotHandler;
import durability.snapshot.SnapshotStrategy.SnapshotStrategy;
import durability.snapshot.SnapshotStream.ImplSnapshotStreamFactory.NIOSnapshotStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.table.BaseTable;
import storage.table.RecordSchema;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemorySnapshotStrategy implements SnapshotStrategy<InMemoryFullSnapshotResources> {
    private static final Logger LOG = LoggerFactory.getLogger(InMemorySnapshotStrategy.class);
    @Nonnull protected Map<String, BaseTable> tables;
    @Nonnull protected SnapshotOptions snapshotOptions;
    @Nonnull protected String snapshotPath;
    @Nonnull protected ConcurrentHashMap<String, InMemoryKvStateInfo> kvStateInformation = new ConcurrentHashMap<>();
    private static final String DESCRIPTION = "Full snapshot of In-Memory Database";

    public InMemorySnapshotStrategy(Map<String, BaseTable> tables, SnapshotOptions snapshotOptions, String snapshotPath) {
        this.tables = tables;
        this.snapshotOptions = snapshotOptions;
        this.snapshotPath = snapshotPath;
    }
    @Override
    public InMemoryFullSnapshotResources syncPrepareResources(long snapshotId, int partitionId) {
        return new InMemoryFullSnapshotResources(snapshotId, partitionId, kvStateInformation, tables);
    }

    @Override
    public void registerTable(String tableName, RecordSchema r) {
        InMemoryKvStateInfo inMemoryKvStateInfo = new InMemoryKvStateInfo(tableName, r);
        this.kvStateInformation.put(tableName, inMemoryKvStateInfo);
    }

    @Override
    public void asyncSnapshot(long snapshotId, int partitionId, FTManager ftManager) throws IOException {
        NIOSnapshotStreamFactory nioSnapshotStreamFactory = new NIOSnapshotStreamFactory(this.snapshotPath, snapshotOptions.getCompressionAlg());
        InMemoryFullSnapshotResources inMemoryFullSnapshotResources = syncPrepareResources(snapshotId, partitionId);
        AsynchronousFileChannel afc = nioSnapshotStreamFactory.createSnapshotStream();
        Attachment attachment = new Attachment(nioSnapshotStreamFactory.getSnapshotPath(), snapshotId, partitionId, afc, ftManager);
        ByteBuffer dataBuffer = inMemoryFullSnapshotResources.createWriteBuffer(snapshotOptions);
        afc.write(dataBuffer, 0, attachment, new SnapshotHandler());
    }

    @Override
    public String getDescription() {
        return DESCRIPTION;
    }

    public static class InMemoryKvStateInfo implements Serializable {
        public final String tableName;
        public final RecordSchema recordSchema;
        public InMemoryKvStateInfo(String tableName, RecordSchema recordSchema) {
            this.tableName = tableName;
            this.recordSchema = recordSchema;
        }
    }
}
