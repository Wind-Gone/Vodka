package benchmark.oltp;/*
 * OLTPTerminal - Terminal emulator code for jTPCC (entity.transactions)
 *
 *
 */

import benchmark.oltp.entity.OLTPData;
import benchmark.oltp.entity.transactions.TxnBasic;
import benchmark.synchronize.HTAPCheck;
import benchmark.synchronize.components.TPRecorder;
import config.CommonConfig;
import org.apache.log4j.Logger;
import utils.math.random.BasicRandom;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;


public class OLTPTerminal implements CommonConfig, Runnable {
    private static Logger log = Logger.getLogger(OLTPTerminal.class);

    private String terminalName;
    private Integer terminalID;
    private Connection conn = null;
    private Statement stmt = null;
    private Statement stmt1 = null;
    private ResultSet rs = null;
    private int terminalWarehouseID, terminalDistrictID;
    private boolean terminalWarehouseFixed;
    private boolean useStoredProcedures;
    private double paymentWeight;
    private double orderStatusWeight;
    private double deliveryWeight;
    private double stockLevelWeight;
    private double receiveGoodsWeight;
    private int limPerMin_Terminal;
    private OLTPClient parent;
    private BasicRandom rnd;
    private int transactionCount = 1;
    private int numTransactions;
    private int numWarehouses;
    private int newOrderCounter;
    private long totalTnxs = 1;
    private StringBuffer query = null;
    private int result = 0;
    private boolean stopRunningSignal = false;
    public static boolean testFreshness = false;
    public static boolean testPerformance = false;


    long terminalStartTime = 0;
    long transactionEnd = 0;

    OLTPConnection db;
    int dbType;
    // htap check variables
    private HTAPCheck htapCheck;
    // tp recorder
    TPRecorder tpRecorder;
    int runningCount = 0;


    public OLTPTerminal
            (String terminalName, int terminalWarehouseID, int terminalDistrictID,
             Connection conn, int dbType,
             int numTransactions, boolean terminalWarehouseFixed,
             boolean useStoredProcedures,
             double paymentWeight, double orderStatusWeight,
             double deliveryWeight, double stockLevelWeight, double receiveGoodsWeight,
             int numWarehouses, int limPerMin_Terminal, HTAPCheck htapCheck, TPRecorder tpRecorder, OLTPClient parent) throws SQLException {
        this.terminalName = terminalName;
        this.terminalID = Integer.parseInt(terminalName.split("-")[1]);      // 获取线程ID
        this.conn = conn;
        this.dbType = dbType;
        this.stmt = conn.createStatement();
        this.stmt.setMaxRows(200);
        this.stmt.setFetchSize(100);
        this.stmt1 = conn.createStatement();
        this.stmt1.setMaxRows(1);
        this.terminalWarehouseID = terminalWarehouseID;
        this.terminalDistrictID = terminalDistrictID;
        this.terminalWarehouseFixed = terminalWarehouseFixed;
        this.useStoredProcedures = useStoredProcedures;
        this.parent = parent;
        this.rnd = parent.getRnd().newRandom();
        this.numTransactions = numTransactions;
        this.paymentWeight = paymentWeight;
        this.orderStatusWeight = orderStatusWeight;
        this.deliveryWeight = deliveryWeight;
        this.stockLevelWeight = stockLevelWeight;
        this.receiveGoodsWeight = receiveGoodsWeight;
        this.numWarehouses = numWarehouses;
        this.newOrderCounter = 0;
        this.limPerMin_Terminal = limPerMin_Terminal;
        this.db = new OLTPConnection(conn, dbType);
        this.htapCheck = htapCheck;
        this.tpRecorder = tpRecorder;
        terminalMessage("");
        terminalMessage("Terminal \'" + terminalName + "\' has WarehouseID=" + terminalWarehouseID + " and DistrictID=" + terminalDistrictID + ".");
        terminalStartTime = System.currentTimeMillis();
        this.testFreshness = parent.getHtapCheck() != null ? parent.getHtapCheck().info.isHtapCheck: false;
        this.testPerformance = parent.getTerminalsAPstarted() > 0;
    }

    public void run() {
        try {
            SimpleDateFormat simFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date startDate = simFormat.parse("1998-08-02 00:00:00");
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(startDate);
            calendar.add(Calendar.SECOND, (int) Math.round(terminalID * OLTPClient.thread_add_interval));   // 按照线程ID，每个线程获取最起始的时间
            Date initialTime = calendar.getTime();
            executeTransactions(numTransactions, initialTime);
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            printMessage("");
            printMessage("Closing entity.statement and connection...");
            stmt.close();
            conn.close();
        } catch (Exception e) {
            printMessage("");
            printMessage("An error occurred!");
            logException(e);
            e.printStackTrace();
        }
        printMessage("");
        printMessage("Terminal \'" + terminalName + "\' finished after " + (transactionCount - 1) + " transaction(s).");
        parent.signalTerminalEnded(this, newOrderCounter);
    }

    public void stopRunningWhenPossible() {
        stopRunningSignal = true;
        printMessage("");
        printMessage("Terminal received stop signal!");
        printMessage("Finishing current transaction before exit...");
    }

    private void executeTransactions(int numTransactions, Date initialTime) {
        boolean stopRunning = false;
        if (numTransactions != -1)
            printMessage("Executing " + numTransactions + " entity.transactions...");
        else
            printMessage("Executing for a limited time...");
        long threadStartTime = System.currentTimeMillis();
        long correct;
        long finishedTxnNum = 0;
        long timePerTx = 60000 / limPerMin_Terminal;
        for (int i = 0; (i < numTransactions || numTransactions == -1) && !stopRunning; i++) {
            double transactionType = rnd.nextDouble(0.0, 100.0);
            int skippedDeliveries = 0, newOrder = 0;
            String transactionTypeName;
            long transactionStart = System.currentTimeMillis();
            if (!terminalWarehouseFixed)
                terminalWarehouseID = rnd.nextInt(1, numWarehouses);
            if (transactionType <= paymentWeight) {
                OLTPData term = new OLTPData();
                term.setNumWarehouses(numWarehouses);
                term.setWarehouse(terminalWarehouseID);
                term.setDistrict(terminalDistrictID);
                term.setUseStoredProcedures(useStoredProcedures);
                term.setDBType(dbType);
                term.setTPRecorder(tpRecorder);
                try {
                    term.generatePayment(log, rnd, 0);
                    term.traceScreen(log);
                    term.execute(log, db, rnd, null);
                    parent.resultAppend(term);
                    term.traceScreen(log);
                } catch (Exception e) {
                    log.fatal(e.getMessage());
                    e.printStackTrace();
                    System.exit(1);
                }
                transactionTypeName = "Payment";
                benchmark.oltp.OLTPClient.payment.getAndIncrement();
            } else if (transactionType <= paymentWeight + stockLevelWeight) {
                OLTPData term = new OLTPData();
                term.setNumWarehouses(numWarehouses);
                term.setWarehouse(terminalWarehouseID);
                term.setDistrict(terminalDistrictID);
                term.setUseStoredProcedures(useStoredProcedures);
                term.setDBType(dbType);
                try {
                    term.generateStockLevel(log, rnd, 0);
                    term.traceScreen(log);
                    term.execute(log, db, rnd, null);
                    parent.resultAppend(term);
                    term.traceScreen(log);
                } catch (Exception e) {
                    log.fatal(e.getMessage());
                    e.printStackTrace();
                    System.exit(1);
                }
                transactionTypeName = "Stock-Level";
                benchmark.oltp.OLTPClient.stockLevel.getAndIncrement();
            } else if (transactionType <= paymentWeight + stockLevelWeight + orderStatusWeight) {
                OLTPData term = new OLTPData();
                term.setNumWarehouses(numWarehouses);
                term.setWarehouse(terminalWarehouseID);
                term.setDistrict(terminalDistrictID);
                term.setUseStoredProcedures(useStoredProcedures);
                term.setDBType(dbType);
                try {
                    term.generateOrderStatus(log, rnd, 0);
                    term.traceScreen(log);
                    term.execute(log, db, rnd, null);
                    parent.resultAppend(term);
                    term.traceScreen(log);
                } catch (Exception e) {
                    log.fatal(e.getMessage());
                    e.printStackTrace();
                    System.exit(1);
                }
                transactionTypeName = "Order-Status";
                benchmark.oltp.OLTPClient.orderStatus.getAndIncrement();
            } else if (transactionType <= paymentWeight + stockLevelWeight + orderStatusWeight + receiveGoodsWeight) {
                OLTPData term = new OLTPData();
                term.setNumWarehouses(numWarehouses);
                term.setWarehouse(terminalWarehouseID);
                term.setDistrict(terminalDistrictID);
                try {
                    term.generateReceiveGoods(log, rnd, 0);
                    term.traceScreen(log);
                    term.execute(log, db, rnd, null);
                    parent.resultAppend(term);
                    term.traceScreen(log);
                } catch (Exception e) {
                    log.fatal(e.getMessage());
                    e.printStackTrace();
                    System.exit(1);
                }
                transactionTypeName = "Receive-Goods";
                benchmark.oltp.OLTPClient.receiveGoods.getAndIncrement();
            } else if (transactionType <= paymentWeight + stockLevelWeight + orderStatusWeight + receiveGoodsWeight + deliveryWeight) {
                OLTPData term = new OLTPData();
                term.setNumWarehouses(numWarehouses);
                term.setWarehouse(terminalWarehouseID);
                term.setDistrict(terminalDistrictID);
                term.setUseStoredProcedures(useStoredProcedures);
                term.setDBType(dbType);
                try {
                    term.generateDelivery(log, rnd, 0);
                    term.traceScreen(log);
                    term.execute(log, db, rnd, null);
                    parent.resultAppend(term);
                    term.traceScreen(log);
                    OLTPData bg = term.getDeliveryBG();
                    bg.traceScreen(log);
                    if (htapCheck != null) {
                        bg.setHtapCheck(htapCheck);
                    }
                    bg.execute(log, db, rnd, null);
                    parent.resultAppend(bg);
                    bg.traceScreen(log);
                    skippedDeliveries = bg.getSkippedDeliveries();
                } catch (Exception e) {
                    log.fatal(e.getMessage());
                    e.printStackTrace();
                    System.exit(1);
                }
                transactionTypeName = "Delivery";
                benchmark.oltp.OLTPClient.DeliveryBG.getAndIncrement();
            } else {
                OLTPData term = new OLTPData();
                term.setNumWarehouses(numWarehouses);
                term.setWarehouse(terminalWarehouseID);
                term.setDistrict(terminalDistrictID);
                term.setUseStoredProcedures(useStoredProcedures);
                term.setDBType(dbType);
                term.setTPRecorder(tpRecorder);
                try {
                    if (htapCheck != null)
                        term.setHtapCheck(htapCheck);
                    term.generateNewOrder(log, rnd, 0);
                    term.traceScreen(log);
                    Calendar tmpcalendar = Calendar.getInstance();
                    tmpcalendar.setTime(initialTime);
                    tmpcalendar.add(Calendar.SECOND, (int) Math.round(newOrderCounter * OLTPClient.TPterminals.length * OLTPClient.thread_add_interval));
                    Date startTime = tmpcalendar.getTime();
                    if (newOrder % 100 == 0)
                        OLTPClient.currTime = startTime;//每隔100个事务更新一次全局的OLTPClient.currTime，修正一次deltadays
                    term.execute(log, db, rnd, startTime);
                    parent.resultAppend(term);
                    term.traceScreen(log);
                } catch (Exception e) {
                    log.fatal(e.getMessage());
                    System.out.println("Error happens in Function@OLTPTerminal.class: executeTransactions");
                    e.printStackTrace();
                    System.exit(1);
                }
                transactionTypeName = "New-Order";
                benchmark.oltp.OLTPClient.newOrder.getAndIncrement();
                newOrderCounter++;
                newOrder = 1;
            }
            long transactionEnd = System.currentTimeMillis();
            if (!transactionTypeName.equals("Delivery")) {
                parent.signalTerminalEndedTransaction(this.terminalName, transactionTypeName, transactionEnd - transactionStart, null, newOrder);
            } else {
                parent.signalTerminalEndedTransaction(this.terminalName, transactionTypeName, transactionEnd - transactionStart, (skippedDeliveries == 0 ? "None" : "" + skippedDeliveries + " delivery(ies) skipped."), newOrder);
            }
            if (limPerMin_Terminal > 0) {
                finishedTxnNum++;
                correct = (transactionEnd - threadStartTime - 1) / timePerTx + 1;
                if (finishedTxnNum == correct) {
                    long sleepTime = timePerTx - (transactionEnd % timePerTx);
                    try {
                        Thread.sleep(sleepTime);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else if (finishedTxnNum > correct) {
                    long sleepTime = timePerTx - (transactionEnd % timePerTx) + (finishedTxnNum - correct) * timePerTx;
                    try {
                        Thread.sleep(sleepTime);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            if (stopRunningSignal) stopRunning = true;
        }
    }

    private void error(String type) {
        log.error(terminalName + ", TERMINAL=" + terminalName + "  TYPE=" + type + "  COUNT=" + transactionCount);
        System.out.println(terminalName + ", TERMINAL=" + terminalName + "  TYPE=" + type + "  COUNT=" + transactionCount);
    }

    private void logException(Exception e) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        e.printStackTrace(printWriter);
        printWriter.close();
        log.error(stringWriter.toString());
    }

    private void terminalMessage(String message) {
        log.trace(terminalName + ", " + message);
    }

    private void printMessage(String message) {
        log.trace(terminalName + ", " + message);
    }

    void transRollback() {
        try {
            conn.rollback();
        } catch (SQLException se) {
            log.error(se.getMessage());
        }
    }

    void transCommit() {
        try {
            conn.commit();
        } catch (SQLException se) {
            log.error(se.getMessage());
            transRollback();
        }
    } // end transCommit()

}