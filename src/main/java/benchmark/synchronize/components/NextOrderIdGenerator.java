package benchmark.synchronize.components;

import java.util.concurrent.atomic.*;
import java.util.*;
import java.sql.*;

// nextOrderIdGenerator is used for generate order id atomically to replace `select .. for update`.
public class NextOrderIdGenerator {
    private HashMap<Integer, HashMap<Integer, AtomicInteger>> generators = new HashMap<>(); // (wid, did) -> order id
    private int warehouseNum;

    public NextOrderIdGenerator(int warehouseNum, Connection conn) {
        this.warehouseNum = warehouseNum;
        int[][] latestOrderId = new int[warehouseNum + 1][11];
        getLatestOrderId(latestOrderId, conn);
        for (int i = 1; i <= warehouseNum; ++i) {
            HashMap<Integer, AtomicInteger> mp = new HashMap<>();
            for (int j = 1; j <= 10; ++j) {
                mp.put(j, new AtomicInteger(latestOrderId[i][j] + 1));
            }
            generators.put(i, mp);
        }
        System.out.println("latest order id:");
        for (int i = 1; i <= warehouseNum; i++) {
            for (int j = 1; j <= 10; j++) {
                System.out.printf("[%d][%d] = %d\n", i, j, latestOrderId[i][j] + 1);
            }
            System.out.println();
        }
        latestOrderId = null;
    }

    private void getLatestOrderId(int[][] latestOrderId, Connection conn) {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(
                "SELECT max(o_id) as max_o_id " +
                "    FROM vodka_oorder " +
                "    WHERE o_w_id = ? AND o_d_id = ? ");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        for (int i = 1; i <= warehouseNum; ++i) {
            for (int j = 1; j <= 10; ++j) {
                try {
                    stmt.setInt(1, i);
                    stmt.setInt(2, j);
                    ResultSet rs = stmt.executeQuery();
                    if (!rs.next()) {
                        rs.close();
                        System.out.println("[getLatestOrderId] Error");
                        System.exit(1);
                    }
                    latestOrderId[i][j] = rs.getInt("max_o_id");
                    rs.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }
    }

    public int getOrderId(int warehouseId, int districtId) {
        return generators.get(warehouseId).get(districtId).getAndIncrement();
    }
}

