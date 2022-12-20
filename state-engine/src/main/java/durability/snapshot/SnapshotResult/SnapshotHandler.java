package durability.snapshot.SnapshotResult;

import java.io.IOException;
import java.nio.channels.CompletionHandler;

public class SnapshotHandler implements CompletionHandler<Integer, Attachment> {
    @Override
    public void completed(Integer result, Attachment attach) {
        try {
            //TODO: create snapshot result and commit to FTManager
            attach.asyncChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void failed(Throwable exc, Attachment attach) {
        try {
            attach.asyncChannel.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }
}
