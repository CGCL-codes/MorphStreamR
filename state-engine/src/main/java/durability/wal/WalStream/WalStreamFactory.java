package durability.wal.WalStream;

import java.io.IOException;
import java.nio.channels.AsynchronousFileChannel;

public interface WalStreamFactory {
    AsynchronousFileChannel createSnapshotStream() throws IOException;
}
