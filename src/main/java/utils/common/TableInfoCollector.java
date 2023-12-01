package utils.common;


import benchmark.olap.OLAPTerminal;
import config.CommonConfig;
import org.apache.log4j.Logger;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TableInfoCollector implements Runnable {
    private static org.apache.log4j.Logger log = Logger.getLogger(TableInfoCollector.class);
    int timeInterval;
    String sConn;
    Properties dbprop;
    Connection conn_tableStaticsCollect = null;
    int dbType;
    int sys_Tps_Limit;
    double neworderWeight;
    double paymentWeight;
    double deliveryWeight;
    double receiveGoodsWeight;
    SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");          //将毫秒级long值转换成日期格式

    public TableInfoCollector(String iConn, Properties idbProps, int testTimeInterval, int dbType, int tps_limit, double ineworderWeight, double ipaymentWeight, double ideliveryWeight, double ireceiveGoodsWeight) {
        this.sConn = iConn;
        this.dbprop = idbProps;
        this.timeInterval = testTimeInterval;
        this.dbType = dbType;
        this.sys_Tps_Limit = tps_limit;
        this.neworderWeight = ineworderWeight;
        this.paymentWeight = ipaymentWeight;
        this.deliveryWeight = ideliveryWeight;
        this.receiveGoodsWeight = ireceiveGoodsWeight;
    }


    public TableInfoCollector(String iConn, Properties dbProps, int dbType, int tps_limit, double ineworderWeight, double ipaymentWeight, double ideliveryWeight, double ireceiveGoodsWeight) {
        this(iConn, dbProps, 5, dbType, tps_limit, ineworderWeight, ipaymentWeight, ideliveryWeight, ireceiveGoodsWeight);
    }

    @Override
    public void run() {
        // 执行信息收集任务的代码,定期收集两个表的数据
        long startTimestamp = System.currentTimeMillis();
        long currentTimestamp = startTimestamp;
        boolean systemNotStopRunning = true;
//        log.info("startTimestamp: " + dateformat.format(new Timestamp(startTimestamp).getTime()));
//        log.info("currentTimestamp: " + dateformat.format(new Timestamp(currentTimestamp).getTime()));
        try {
            long delta;
            do {
                //    log.info("开始执行信息收集任务...");
                conn_tableStaticsCollect = DriverManager.getConnection(sConn, dbprop);
                updateTableStaticsStatusLine(conn_tableStaticsCollect);
                conn_tableStaticsCollect.close();
                //     log.info("信息收集任务执行完成。开始更新状态信息。。");
                startTimestamp = currentTimestamp;
                if (benchmark.oltp.OLTPClient.getSignalTerminalsRequestEndSent())
                    systemNotStopRunning = false;
                do {
                    currentTimestamp = System.currentTimeMillis();
                    delta = currentTimestamp - startTimestamp;
                } while ((delta < 5000) && (systemNotStopRunning));
            } while ((delta >= 5000) && (systemNotStopRunning));
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    synchronized public void updateTableStaticsStatusLine(Connection dconn) throws SQLException {
        //may be can be changed
        int mode = 1;   //mode=0 use select count table-real ,mode=1 use tps plus time count table-estimate
        if (mode == 0) {
            String sql1, sql2;
            if (dbType == CommonConfig.DB_TIDB) {
                sql1 = "SELECT /*+ read_from_storage(tiflash[vodka_oorder]) */ count(*) AS oorderTableSize FROM vodka_oorder";
                sql2 = "SELECT /*+ read_from_storage(tiflash[vodka_order_line]) */ count(*) AS OrderlineTableSize FROM vodka_order_line";
            } else {
                sql1 = "SELECT count(*) AS oorderTableSize FROM vodka_oorder";
                sql2 = "SELECT count(*) AS OrderlineTableSize FROM vodka_order_line";
            }
            PreparedStatement stmt1 = dconn.prepareStatement(sql1);
            ResultSet rs1 = stmt1.executeQuery();
            if (!rs1.next())
                throw new SQLException("get oorder table size error!");
            OLAPTerminal.oorderTableSize = new AtomicLong(rs1.getInt("oorderTableSize"));
            System.out.println("init OLAPTerminal.oorderTableSize:" + OLAPTerminal.oorderTableSize);
            double[] xy1 = {System.currentTimeMillis() - benchmark.oltp.OLTPClient.gloabalSysCurrentTime, OLAPTerminal.oorderTableSize.get()};
            rs1.close();
            benchmark.oltp.OLTPClient.generalOorderTSize.add(xy1);

            PreparedStatement stmt2 = dconn.prepareStatement(sql2);
            ResultSet rs2 = stmt2.executeQuery();
            if (!rs2.next())
                throw new SQLException("get orderline table size error!");
            OLAPTerminal.orderLineTableSize = new AtomicLong(rs2.getInt("OrderlineTableSize"));
            System.out.println("init OLAPTerminal.orderLineTableSize:" + OLAPTerminal.orderLineTableSize);
            double[] xy2 = {System.currentTimeMillis() - benchmark.oltp.OLTPClient.gloabalSysCurrentTime, OLAPTerminal.orderLineTableSize.get()};
            rs2.close();
            benchmark.oltp.OLTPClient.generalOrderlineTSize.add(xy2);
        } else {
            long time = (System.currentTimeMillis() - benchmark.oltp.OLTPClient.gloabalSysCurrentTime);
            double timePerMin = time / (1000.0 * 60);
//            log.info("####time:" + time + ",timePerMin:" + timePerMin);//benchmark.oltp.OLTPClient.recentTpmTotal
            // OLAPTerminal.oorderTableSize = (int) (benchmark.olap.query.baseQuery.orderOriginSize + (benchmark.oltp.OLTPClient.transactionCount-benchmark.oltp.OLTPClient.rollBackTransactionCount)  * neworderWeight / 100.0 * 1);//order表仅neworder事务会进行新增,1个neworder事务插入order表1行
            // OLAPTerminal.oorderTableSize = (int) (benchmark.olap.query.baseQuery.orderOriginSize + (benchmark.oltp.OLTPClient.newOrder-benchmark.oltp.OLTPClient.rollBackTransactionCount)  * 1);
            double[] xy1 = {time, OLAPTerminal.oorderTableSize.get()};

            benchmark.oltp.OLTPClient.generalOorderTSize.add(xy1);

            // OLAPTerminal.orderLineTableSize = (int) (benchmark.olap.query.baseQuery.olOriginSize + (benchmark.oltp.OLTPClient.transactionCount-benchmark.oltp.OLTPClient.rollBackTransactionCount) * neworderWeight / 100.0 * 10);//orderline表仅neworder事务进行新增，1个neworder事务插入5~15行orderline，平均10行
            // OLAPTerminal.orderLineTableSize = (int) (benchmark.olap.query.baseQuery.olOriginSize + (benchmark.oltp.OLTPClient.newOrder-benchmark.oltp.OLTPClient.rollBackTransactionCount) * 10);

            double[] xy2 = {time, OLAPTerminal.orderLineTableSize.get()};
            benchmark.oltp.OLTPClient.generalOrderlineTSize.add(xy2);

            // OLAPTerminal.orderlineTableRecipDateNotNullSize = (int) (benchmark.olap.query.baseQuery.olNotnullSize + benchmark.oltp.OLTPClient.transactionCount * receiveGoodsWeight / 100.0 * 10 *10);//orderline表仅delivery事务新增null值为notnull，1个delivery事务10个district，每个district更新5~15行orderline，平均100行
            // OLAPTerminal.orderlineTableRecipDateNotNullSize = (int) (benchmark.olap.query.baseQuery.olNotnullSize + benchmark.oltp.OLTPClient.receiveGoods* 10 *10);

            // OLAPTerminal.orderlineTableNotNullSize = (int) (benchmark.olap.query.baseQuery.olNotnullSize + benchmark.oltp.OLTPClient.transactionCount* deliveryWeight / 100.0 * 10 * 10);//orderline表仅delivery事务新增null值为notnull，1个delivery事务10个district，每个district更新5~15行orderline，平均100行
            // OLAPTerminal.orderlineTableNotNullSize = (int) (benchmark.olap.query.baseQuery.olNotnullSize + benchmark.oltp.OLTPClient.DeliveryBG * 10 * 10);
            // log.info("benchmark.oltp.OLTPClient.recentTpmTotal:"+benchmark.oltp.OLTPClient.recentTpmTotal);
            // log.info("benchmark.oltp.OLTPClient.transactionCount:"+benchmark.oltp.OLTPClient.transactionCount);

            // output information
//            log.info("benchmark.oltp.OLTPClient.transactionCount:"+benchmark.oltp.OLTPClient.transactionCount);
//            log.info("benchmark.oltp.OLTPClient.rollBackTransactionCount:"+benchmark.oltp.OLTPClient.rollBackTransactionCount.get());
//            log.info("OLAPTerminal.oorderTableSize:"+OLAPTerminal.oorderTableSize.get());
//            log.info("OLAPTerminal.orderLineTableSize:"+OLAPTerminal.orderLineTableSize.get());
//            log.info("OLAPTerminal.orderlineTableNotNullSize:"+OLAPTerminal.orderlineTableNotNullSize.get());
//            log.info("OLAPTerminal.orderlineTableRecipDateNotNullSize:"+OLAPTerminal.orderlineTableRecipDateNotNullSize.get());
//            log.info("2405+OLTPClient.deltaDays2:"+(2405+benchmark.oltp.OLTPClient.deltaDays2));
//            log.info("benchmark.oltp.OLTPClient.DeliveryBG:"+benchmark.oltp.OLTPClient.DeliveryBG.get());
//            log.info("benchmark.oltp.OLTPClient.newOrder:"+benchmark.oltp.OLTPClient.newOrder.get());
//            log.info("benchmark.oltp.OLTPClient.orderStatus:"+benchmark.oltp.OLTPClient.orderStatus.get());
//            log.info("benchmark.oltp.OLTPClient.payment:"+benchmark.oltp.OLTPClient.payment.get());
//            log.info("benchmark.oltp.OLTPClient.receiveGoods:"+benchmark.oltp.OLTPClient.receiveGoods.get());
//            log.info("benchmark.oltp.OLTPClient.stockLevel:"+benchmark.oltp.OLTPClient.stockLevel.get());
        }
    }
}
