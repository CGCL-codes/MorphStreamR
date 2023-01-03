package durability.recovery;

import durability.struct.Result.persistResult;

import java.util.ArrayList;
import java.util.List;

public class RedoLogResult implements persistResult {
    public List<String> redoLogPaths = new ArrayList<>();
    public long lastedGroupId;
    public void addPath(String path) {
        this.redoLogPaths.add(path);
    }

    public void setLastedGroupId(long lastedGroupId) {
        this.lastedGroupId = lastedGroupId;
    }
}
