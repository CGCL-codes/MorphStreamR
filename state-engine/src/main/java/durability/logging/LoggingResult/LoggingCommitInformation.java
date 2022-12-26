package durability.logging.LoggingResult;

import java.io.Serializable;
import java.util.List;
import java.util.Vector;

public class LoggingCommitInformation implements Serializable {
    public final long groupId;
    public final List<LoggingResult> loggingResults = new Vector<>();

    public LoggingCommitInformation(long groupId) {
        this.groupId = groupId;
    }
}
