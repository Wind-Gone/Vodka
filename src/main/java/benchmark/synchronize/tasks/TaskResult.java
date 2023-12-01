package benchmark.synchronize.tasks;

public class TaskResult {
    public int taskType;
    public long txnCompleteTime;
    public long gapTime;
    public long startTime;
    public long endTime;
    public int tryNum;
    public boolean pass;
    public boolean isApConnErr;

    public TaskResult(int taskType, long txnCompleteTime, long gapTime, long startTime, long endTime,
                        int tryNum, boolean pass, boolean isApConnErr) {
        this.taskType = taskType;
        this.txnCompleteTime = txnCompleteTime;
        this.gapTime = gapTime;
        this.startTime = startTime;
        this.endTime = endTime;
        this.tryNum = tryNum;
        this.pass = pass;
        this.isApConnErr = isApConnErr;
    }
}
