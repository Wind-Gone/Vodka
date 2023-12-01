package benchmark.synchronize.tasks;

import java.sql.*;
import java.util.*;

abstract public class Task {
    // datebase
    protected ResultSet result;

    // time
    protected long startTime;
    protected long endTime;
    protected long txnCompleteTime;
    protected long gapTime;

    // task type
    public int taskType;

    // run function
    public abstract TaskResult runTask(ArrayList<Connection> conns, int threadId);
}