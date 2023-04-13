package durability.struct.Logging;

import content.common.CommonMetaTypes;
import scheduler.struct.MetaTypes;

import java.util.ArrayList;

public class LVCLog extends CommandLog{
    public int threadId;
    public CommonMetaTypes.AccessType accessType;
    public boolean isAborted = false;
    private int[] LVs;
    public LVCLog(long LSN, String tableName, String key, String OperationFunction, String[] conditions, String parameter) {
        super(LSN, tableName, key, OperationFunction, conditions, parameter);
    }
    public void setThreadId(int threadId) {
        this.threadId = threadId;
    }
    public void setAccessType(CommonMetaTypes.AccessType accessType) {
        this.accessType = accessType;
    }
    public void setLVs(int[] LVs) {
        this.LVs = LVs;
    }
    public void setLSN(long LSN) {
        this.LSN = LSN;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(LSN).append(";");//LSN -0
        for (int i : LVs) {
            stringBuilder.append(i).append(",");//LVs -1
        }
        stringBuilder.append(";");
        stringBuilder.append(tableName).append(";");//tableName -2
        stringBuilder.append(key).append(";");//key -3
        for (String ckeys : condition) {
            stringBuilder.append(ckeys).append(",");//condition -4
        }
        stringBuilder.append(";");
        stringBuilder.append(OperationFunction).append(";");//OperationFunction -5
        stringBuilder.append(parameter.toString()).append(";");//parameter -6
        if (isAborted) {
            stringBuilder.append(1).append(";");
        } else {
            stringBuilder.append(0).append(";");
        }
        return stringBuilder.toString();
    }
    public static LVCLog getLVCLogFromString(String log) {
        String[] logParts = log.split(";");
        ArrayList<String> conditions = new ArrayList<>();
        for (String c : logParts[4].split(",")) {
            if (c.equals(""))
                continue;
            conditions.add(c);
        }
        LVCLog lvcLog = new LVCLog(Long.parseLong(logParts[0]), logParts[2], logParts[3], logParts[5], conditions.toArray(new String[0]), logParts[6]);
        String[] LVs = logParts[1].split(",");
        int[] LVsInt = new int[LVs.length];
        for (int i = 0; i < LVs.length; i++) {
            LVsInt[i] = Integer.parseInt(LVs[i]);
        }
        lvcLog.setLVs(LVsInt);
        if (Integer.parseInt(logParts[7]) == 1) {
            lvcLog.isAborted = true;
        }
        return lvcLog;
    }

    @Override
    public void setVote(MetaTypes.OperationStateType vote) {
        if (vote == MetaTypes.OperationStateType.ABORTED)
            isAborted = true;
    }
}
