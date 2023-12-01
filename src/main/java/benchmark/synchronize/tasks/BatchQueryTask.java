package benchmark.synchronize.tasks;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Lock;

import bean.Triple;
import benchmark.synchronize.components.HTAPCheckInfo;
import benchmark.synchronize.components.HTAPCheckType;
import config.CommonConfig;
import org.apache.commons.math3.util.Pair;

import static benchmark.oltp.OLTPClient.*;

public class BatchQueryTask extends Task {
    // task info
    private final int o_id;
    private final int d_id;
    private final int w_id;
    private final int start_wid;
    private final int end_wid;
    private final long ol_delivery_d;
    private final int freshnessDataBound;
    private final long currentTime;
    public int lFreshLagBound;
    public int rFreshLagBound;

    // parallel sample
    public Lock parallelThreads = null;

    // datebase statement
    private PreparedStatement stmtTP;
    private PreparedStatement stmtAP;
    private ResultSet resultSetAP;
    private ResultSet resultSetTP;
    private String checkSql4TP = "SELECT sum(access_version)" +
            "  FROM vodka_order_line " +
            "  WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;";
    private String checkSql4AP = checkSql4TP;
    private String inClause;
    private String batchQuerySql;
    private String tidbSql;
    private int dbType;
    private boolean isWeakRead;
    private int weakReadTime;

    Random random = new Random(2023);

    public BatchQueryTask(int o_id, int d_id, int w_id, long ol_delivery_d, int dbType, HTAPCheckInfo htapCheckInfo,
            long currentTime) {
        this.taskType = HTAPCheckType.BATCH_QUERY;
        this.txnCompleteTime = System.currentTimeMillis();
        this.o_id = o_id;
        this.d_id = d_id;
        this.w_id = w_id;
        this.ol_delivery_d = ol_delivery_d;
        this.start_wid = random.nextInt(numWarehouses - htapCheckInfo.warehouseNum + 1);
        this.end_wid = start_wid + htapCheckInfo.warehouseNum;
        this.gapTime = htapCheckInfo.gapTime;
        this.freshnessDataBound = htapCheckInfo.htapCheckFreshnessDataBound;
        this.lFreshLagBound = htapCheckInfo.lFreshLagBound;
        this.rFreshLagBound = htapCheckInfo.rFreshLagBound;
        this.currentTime = currentTime;
        this.isWeakRead = htapCheckInfo.isWeakRead;
        this.weakReadTime = htapCheckInfo.weakReadTime;
        inClause = buildInClause(freshnessDataBound);
        batchQuerySql = "SELECT /*+READ_CONSISTENCY(WEAK) */ *" +
                "   FROM vodka_order_line" +
                "   WHERE" +
                "   ol_w_id >= ? AND" +
                "   ol_w_id <= ? AND ol_delivery_d " +
                " IN (" + inClause + ")";
        this.dbType = dbType;
        initSQLs(dbType);
    }

    private void initSQLs(int dbType) {
        if (dbType == CommonConfig.DB_TIDB) {
            checkSql4AP = "SELECT /*+ read_from_storage(tiflash[vodka_order_line]) */ sum(access_version)" +
                    "  FROM vodka_order_line " +
                    "  WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;";
            if (this.isWeakRead) {
                checkSql4AP = "SELECT /*+ read_from_storage(tiflash[vodka_order_line]) */ sum(access_version)" +
                        "  FROM vodka_order_line AS OF TIMESTAMP TIDB_BOUNDED_STALENESS(NOW() - INTERVAL 5 SECOND, NOW()) "
                        +
                        "  WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;";
            }

            tidbSql = "set SESSION tidb_isolation_read_engines = \"tiflash\"";
            batchQuerySql = "SELECT /*+ read_from_storage(tiflash[vodka_order_line]) */ *" +
                    " FROM vodka_order_line " +
                    " WHERE" +
                    " ol_w_id >= ? AND" +
                    " ol_w_id <= ? AND ol_delivery_d " +
                    " IN (" + inClause + ")";

        }
        if (dbType == CommonConfig.DB_OCEANBASE && this.isWeakRead) {
            System.out.println("Isweakread for OB!!!");
            checkSql4AP = "SELECT /*+READ_CONSISTENCY(WEAK) */ sum(access_version)" +
                    "  FROM vodka_order_line " +
                    "  WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?;";
        }
    }

    private String buildInClause(int freshnessDataBound) {
        long lowerBound = currentTime - freshnessDataBound;
        List<String> dateStrings = new ArrayList<>();
        Iterator<Pair<Long, Long>> iterator = deliveryList.descendingIterator();
        while (iterator.hasNext()) {
            Pair<Long, Long> pair = iterator.next();
            if (pair.getKey() <= currentTime) {
                if (pair.getKey() >= lowerBound) {
                    dateStrings.add("'" + new Timestamp(pair.getValue()) + "'");
                } else
                    break;
            }
        }
        if (!dateStrings.isEmpty()) {
            return String.join(", ", dateStrings);
        }
        return "''"; // 默认情况，如果没有匹配项
    }

    @Override
    public TaskResult runTask(ArrayList<Connection> conns, int threadId) {
        if (htapCheckQueryNumber.get() <= 0)
            printFreshnessReport();
        System.out.printf("Remain #%d Real-time query\n", htapCheckQueryNumber.decrementAndGet());
        int tryNum = 0;
        boolean pass = true;
        boolean isApConnErr = false;
        boolean resultMatch = false;
        // try {
        // // prepare sql for tp
        // stmtTP = conns.get(1).prepareStatement(checkSql4TP);
        // stmtTP.setInt(1, w_id);
        // stmtTP.setInt(2, d_id);
        // stmtTP.setInt(3, o_id);
        // // prepare sql for ap
        // stmtAP = conns.get(0).prepareStatement(checkSql4AP);
        // stmtAP.setInt(1, w_id);
        // stmtAP.setInt(2, d_id);
        // stmtAP.setInt(3, o_id);
        // resultSetTP = stmtTP.executeQuery();
        // int resultTP = Integer.MAX_VALUE;
        // if (resultSetTP.next())
        // resultTP = resultSetTP.getInt(1);
        // if (dbType == CommonConfig.DB_TIDB) {
        // Statement ps = conns.get(0).createStatement();
        // ps.execute(tidbSql);
        // }
        // startTime = System.nanoTime();
        // if (isWeakRead) {
        // if (dbType == CommonConfig.DB_OCEANBASE) {
        // Statement weakStatement = conns.get(0).createStatement();
        // weakStatement.execute("SET ob_read_consistency = WEAK;");
        // } else if (dbType == CommonConfig.DB_TIDB) {
        // Statement weakStatement = conns.get(0).createStatement();
        // weakStatement.execute("set @@tidb_read_staleness=\"-5\";");
        // }
        // }
        // while (!resultMatch) {
        // resultSetAP = stmtAP.executeQuery();
        // if (resultSetAP.next()) {
        // int resultAP = resultSetAP.getInt(1);
        // if (resultAP >= resultTP) {
        // System.out.printf("[BATCH_QUERY] [%d] version in AP is [%d], and version in
        // TP is [%d]",
        // threadId, resultAP, resultTP);
        // txnCompleteTime = System.nanoTime();
        // resultMatch = true;
        // } else {
        // tryNum++;
        // if (tryNum % 10000 == 0)
        // System.out.printf(
        // "[BATCH_QUERY] [%d] version in AP is [%d], and version in TP is [%d],
        // checksql is [%s]\n",
        // threadId, resultAP, resultTP, stmtAP.toString() + ";");
        // }
        // }
        // }
        // resultSetAP.close();
        // stmtAP.close();
        // resultSetTP.close();
        // stmtTP.close();

        // } catch (Exception e) {
        // e.printStackTrace();
        // }
        System.out.println("Check Done and Start Second Query!");
        try {
            double freshness = 0;
            double tmpLatency = 0;
            // double freshness = tryNum == 0 ? 0
            // : Math.abs(txnCompleteTime - startTime) /
            // 1_000_000.0;
            // double tmpLatency = tryNum == 0 ? Math.abs(txnCompleteTime - startTime) /
            // 1_000_000.0 : 0;
            stmtAP = conns.get(0).prepareStatement(batchQuerySql);
            stmtAP.setInt(1, start_wid);
            stmtAP.setInt(2, end_wid);
            Statement weakStatement = conns.get(0).createStatement();
            if (dbType == CommonConfig.DB_OCEANBASE && this.isWeakRead) {
                weakStatement.execute("SET ob_read_consistency = WEAK;");
            }
            startTime = System.nanoTime();
            result = stmtAP.executeQuery();
            txnCompleteTime = System.nanoTime();
            if (result.next()) {
                System.out.println(result.getString(1));
            }
            System.out.println(batchQuerySql);
            double latency = Math.abs(txnCompleteTime - startTime) / 1_000_000.0;
            Triple<Double, Double, Double> freshnessItem = new Triple<>(freshness, latency, tmpLatency);
            freshnessTime.add(freshnessItem);
            System.out.printf(
                    "[BATCH_QUERY] [%d] Freshness is [%f]ms for this Database, Latency is [%f]ms, unified linear latency is [%f]ms\n",
                    threadId, freshness, latency, freshness + latency);
            Thread.sleep(3000);
        } catch (SQLException e) {
            isApConnErr = true;
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new TaskResult(taskType, txnCompleteTime, gapTime, startTime, endTime, tryNum, pass, isApConnErr);
    }

    private void printFreshnessReport() {
        int failNumber = 0;
        double totalUnifiedLatency = 0.0;
        double totalFreshness = 0.0;
        double totalLatency = 0.0;
        double totalTmpLatency = 0.0;
        double minFreshness = Double.MAX_VALUE;
        double maxFreshness = Double.MIN_VALUE;

        System.out.println("Freshness | TmpLatency | Latency | Unified Latency");
        freshnessTime.remove(0);

        for (Triple<Double, Double, Double> item : freshnessTime) {
            double freshness = item.getFirst();
            double latency = item.getSecond();
            double tmplatency = item.getThird();
            double unifiedLatency = freshness + latency + tmplatency;
            // Update min and max freshness
            minFreshness = Math.min(minFreshness, freshness);
            maxFreshness = Math.max(maxFreshness, freshness);
            System.out.println(freshness + ", " + tmplatency + ", " + latency + ", " + unifiedLatency);
            totalUnifiedLatency += unifiedLatency;
            totalFreshness += freshness;
            totalLatency += latency;
            totalTmpLatency += tmplatency;
            if (freshness > rFreshLagBound)
                failNumber++;
        }

        double averageUnifiedLatency = totalUnifiedLatency / freshnessTime.size();
        double averageFreshness = totalFreshness / freshnessTime.size();
        double averageLatency = totalLatency / freshnessTime.size();
        double averageTmpLatency = totalTmpLatency / freshnessTime.size();
        double failRate = (double) failNumber / freshnessTime.size();

        System.out.printf("Freshness Fail Rate is: %.2f%%\n", failRate * 100);
        System.out.printf("Average Freshness is: %.2f\n", averageFreshness);
        System.out.printf("Min Freshness is: %.2f\n", minFreshness);
        System.out.printf("Max Freshness is: %.2f\n", maxFreshness);
        System.out.printf("Average TmpLatency is: %.2f\n", averageTmpLatency);
        System.out.printf("Average Latency is: %.2f\n", averageLatency);
        System.out.printf("Average Unified Latency is: %.2f\n", averageUnifiedLatency);

        signalTerminalsRequestEnd(false);
    }
}