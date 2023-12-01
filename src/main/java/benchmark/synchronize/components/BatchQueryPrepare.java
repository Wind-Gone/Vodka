package benchmark.synchronize.components;

import java.sql.*;

public class BatchQueryPrepare {
    public int startOrderId;
    public int endOrderId;
    private int w_id;
    private int d_id;
    private Connection conn;
    private int threshold;
    private int threadId;
    private String getLatestOrderIdSql = "SELECT max(ol_o_id) AS latest_oid" +
                                         "   FROM vodka_order_line" +
                                         "   WHERE ol_w_id = ? AND ol_d_id = ? AND ol_delivery_d > to_timestamp('1971-01-01 00:00:00.000', 'YYYY-MM-DD HH24:MI:SS.MS')";

    public BatchQueryPrepare(int w_id, int d_id, Connection conn, int threadId) {
        this.w_id = w_id;
        this.d_id = d_id;
        this.conn = conn;
        this.startOrderId = getLatestOrderId(w_id, d_id);
        this.endOrderId = 0;
        this.threshold = 50;
        this.threadId = threadId;
    }

    public void preparing() {
        while (endOrderId - startOrderId < threshold) {
            try {
                Thread.sleep(10000); // sleep 10 seconds
            } catch (Exception e) {
                e.printStackTrace();
            }
            endOrderId = getLatestOrderId(w_id, d_id);
            System.out.printf("[preparing] [%d] %d/%d\n", threadId, endOrderId - startOrderId, threshold);
        }
        System.out.printf("[preparing] [%d] done\n", threadId, endOrderId - startOrderId, threshold);
    }

    private int getLatestOrderId(int w_id, int d_id) {
        int latestOrderId = 0;
        while (true) {
            try {
                PreparedStatement stmt = conn.prepareStatement(getLatestOrderIdSql);
                stmt.setInt(1, w_id);
                stmt.setInt(2, d_id);
                ResultSet rs = stmt.executeQuery();
                if (!rs.next()) {
                    rs.close();
                    System.out.println("[getLatestOrderId] Error");
                }
                latestOrderId = rs.getInt("latest_oid");
                rs.close();
            } catch (Exception e) {
                // retry
                System.out.printf("%d retry\n", threadId);
                e.printStackTrace();
                try {
                    // System.out.printf("%d retry\n", threadId);
                    Thread.sleep(1000);
                } catch (Exception useless) {
                    useless.printStackTrace();
                }
                continue;
            }
            break;
        }
        return latestOrderId;
    }
}
