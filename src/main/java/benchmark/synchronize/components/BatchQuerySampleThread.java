package benchmark.synchronize.components;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.*;

public class BatchQuerySampleThread extends Thread {
    private PreparedStatement stmt;
    private Random rnd;
    private ResultSet result;
    private int startOid;
    private int endOid;
    private AtomicInteger success;
    private AtomicInteger finished;
    private AtomicInteger masterState;
    private AtomicInteger tryNum;
    private int sampleWidth;
    private boolean isMaster;
    private int threadId;

    public BatchQuerySampleThread(PreparedStatement stmt, int startOid, int endOid, AtomicInteger success, AtomicInteger finished,
                                  AtomicInteger masterState, int sampleWidth, boolean isMaster, int threadId, AtomicInteger tryNum) {
        this.sampleWidth = sampleWidth;
        this.isMaster = isMaster;
        this.stmt = stmt;
        this.startOid = startOid;
        this.endOid = endOid;
        this.success = success;
        this.finished = finished;
        this.tryNum = tryNum;
        this.masterState = masterState;
        this.sampleWidth = sampleWidth;
        this.isMaster = isMaster;
        this.threadId = threadId;
        this.rnd = new Random();
    }

    public void run() {
        while (true) {
            try {
                int o_id = rnd.nextInt(endOid - startOid) + startOid;
                stmt.setInt(3, o_id);
                result = stmt.executeQuery();
                if (result.next()) {
                    int accessVersion = result.getInt(1);
                    if (accessVersion == 99) {
                        success.incrementAndGet();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            while (masterState.get() == 1) {
                // a next sample, clear first.
                masterState.set(0);
            }

            finished.incrementAndGet();

            // followers need to wait until master done.
            while (masterState.get() == 0) {
                if (isMaster) {
                    // master need to wait util all queries done.
                    while (finished.get() < sampleWidth) {
                        try {
                            Thread.sleep(1);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    tryNum.incrementAndGet();
                    // all queries done, check whether we need a next sample.
                    if (success.get() < sampleWidth) {
                        // need a next sample.
                        finished.set(0);
                        success.set(0);
                        masterState.set(1);
                    } else {
                        // sample done.
                        masterState.set(2);
                    }
                } else {
                    try {
                        Thread.sleep(1);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            if (masterState.get() == 2) {
                break;
            }

            try {
                Thread.sleep(10);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
