package benchmark.synchronize.tasks;

import java.sql.*;
import java.util.*;

import benchmark.synchronize.components.HTAPCheckType;

public class AdHocUpdateTask extends Task {
    // task info
    private int o_id;
    private int d_id;
    private int w_id;

    // datebase statement
    private PreparedStatement stmt;
    private String getSuccessNumSql = "SELECT sum(access_version)" +
                         "  FROM vodka_order_line " +
                         "  WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?";

    public AdHocUpdateTask(int o_id, int d_id, int w_id, long gapTime) {
        this.taskType = HTAPCheckType.AD_HOC_UPDATE;
        this.txnCompleteTime = System.currentTimeMillis();
        this.gapTime = gapTime;
        this.o_id = o_id;
        this.d_id = d_id;
        this.w_id = w_id;
    }

    @Override
    public TaskResult runTask(ArrayList<Connection> conns, int threadId) {
        int tryNum = 0;
        boolean pass = true;
        boolean isApConnErr = false;
        long delta = 0;
        try {
            // prepare sql
            stmt = conns.get(0).prepareStatement(getSuccessNumSql);
            stmt.setInt(1, w_id);
            stmt.setInt(2, d_id);
            stmt.setInt(3, o_id);

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
                result = stmt.executeQuery();
                tryNum++;
                if (result.next()) {
                    int successNum = result.getInt(1);
                    endTime = System.currentTimeMillis();
                    result.close();
                    if (successNum != 0) {
                        delta = endTime - startTime;
                        break;
                    }
                }
                pass = false;
                Thread.sleep(10); // sleep 10ms
                if (tryNum >= 1000) {
                    System.out.printf("[AD_HOC_UPDATE] try num >= 1000!!!!!!!!!!!!!!!");
                    break;
                }
            }
        } catch (SQLException e) {
            isApConnErr = true;
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.printf("[AD_HOC_UPDATE] [%d] delta = %dms, tryNum = %d\n", threadId, delta, tryNum);
        return new TaskResult(taskType, txnCompleteTime, gapTime, startTime, endTime, tryNum, pass, isApConnErr);
    }
}