package durability.logging.LoggingResult;

import utils.lib.ConcurrentHashMap;

import java.io.Serializable;
import java.util.List;
import java.util.Vector;

public class LoggingCommitInformation implements Serializable {
    public final long groupId;
    public final ConcurrentHashMap<Integer, LoggingResult> loggingResults = new ConcurrentHashMap<>();

    public LoggingCommitInformation(long groupId) {
        this.groupId = groupId;
    }
}
