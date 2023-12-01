package benchmark.synchronize.tasks;

import java.sql.*;
import java.util.*;

import benchmark.synchronize.components.HTAPCheckType;

public class AdHocInsertTask extends Task {
    // task info
    private int o_id;
    private int d_id;
    private int w_id;
    private int ol_cnt;

    // datebase statement
    private PreparedStatement stmtNewOrderSelectAccessVersion;
    private String sql = "SELECT sum(access_version)" +
                         "  FROM vodka_order_line " +
                         "  WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?";

    public AdHocInsertTask(int o_id, int d_id, int w_id, int ol_cnt, long gapTime) {
        this.taskType = HTAPCheckType.AD_HOC_INSERT;
        this.txnCompleteTime = System.currentTimeMillis();
        this.gapTime = gapTime;
        this.o_id = o_id;
        this.d_id = d_id;
        this.w_id = w_id;
        this.ol_cnt = ol_cnt;
    }

    @Override
    public TaskResult runTask(ArrayList<Connection> conns, int threadId) {
        int tryNum = 0;
        boolean pass = true;
        boolean isApConnErr = false;
        int sum = 0;
        long delta = 0;
        try {
            // prepare sql
            stmtNewOrderSelectAccessVersion = conns.get(0).prepareStatement(sql);
            stmtNewOrderSelectAccessVersion.setInt(1, w_id);
            stmtNewOrderSelectAccessVersion.setInt(2, d_id);
            stmtNewOrderSelectAccessVersion.setInt(3, o_id);

            // wait gap
            while (true) {
                startTime = System.currentTimeMillis();
                if (startTime - txnCompleteTime >= gapTime) {
                    break;
                }
                try {
                    Thread.sleep(1);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            // check
            while (true) {
                result = stmtNewOrderSelectAccessVersion.executeQuery();
                tryNum++;
                if (result.next()) {
                    sum = result.getInt(1);
                    endTime = System.currentTimeMillis();
                    result.close();
                    if (sum == ol_cnt) {
                        delta = endTime - startTime;
                        break;
                    }
                }
                pass = false;
                Thread.sleep(10); // sleep 10ms
                if (tryNum >= 1000) {
                    System.out.printf("[AD_HOC_INSERT] try num >= 1000!");
                    break;
                }
            }
        } catch (SQLException e) {
            isApConnErr = true;
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.printf("[AD_HOC_INSERT] [%d] %d/%d, delta = %dms, tryNum = %d\n", threadId, sum, ol_cnt, delta, tryNum);
        return new TaskResult(taskType, txnCompleteTime, gapTime, startTime, endTime, tryNum, pass, isApConnErr);
    }
}