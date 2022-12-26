package durability.logging.LoggingResult;

import durability.snapshot.SnapshotResult.SnapshotResult;
import utils.FaultToleranceConstants;

import java.io.IOException;
import java.nio.channels.CompletionHandler;

public class LoggingHandler implements CompletionHandler<Integer, Attachment> {

    @Override
    public void completed(Integer result, Attachment attach) {
        try {
            LoggingResult loggingResult = attach.getLoggingResult();
            attach.ftManager.boltRegister(attach.partitionId,
                    FaultToleranceConstants.FaultToleranceStatus.Persist, loggingResult);
            attach.asyncChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void failed(Throwable exc, Attachment attach) {
        try {
            //TODO: notify the operator to reExecute snapshot
            attach.asyncChannel.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }
}
