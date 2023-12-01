package benchmark.synchronize.components;

import java.util.concurrent.atomic.*;
import java.util.Properties;

import benchmark.oltp.entity.statement.*;


import java.sql.*;

public class TPRecorder {
    private boolean isFinished;
    private long lastNewOrder;
    private long lastPayment;
    private AtomicLong newOrder;
    private AtomicLong payment;
    private Connection dbConn;
    private StmtVodkaTime stmtVodkaTime;
    private PreparedStatement stmt;
    private ResultSet result;
    private Thread thread;

    public TPRecorder(int dbType, String database, Properties dbProps) {
        this.isFinished = false;
        try {
            dbConn = DriverManager.getConnection(database, dbProps);
            this.stmtVodkaTime = new StmtVodkaTime(dbConn, dbType);
            stmt = stmtVodkaTime.getStmtGetCurrentVodkaTime();
            result = stmt.executeQuery();
            if (result.next()) {
                lastNewOrder = result.getLong(1);
                lastPayment = result.getLong(2);
            }
            result.close();
            this.newOrder = new AtomicLong(lastNewOrder);
            this.payment = new AtomicLong(lastPayment);
        } catch (Exception e) {
            e.printStackTrace();
        }

        thread = new Thread(() -> updateVodkaTime());
        thread.start();
    }

    public void updateVodkaTime() {
        while (!isFinished) {
            long curNewOrder = newOrder.get();
            long curPayment = payment.get();
            long deltaNewOrder = curNewOrder - lastNewOrder;
            long deltaPayment = curPayment - lastPayment;
            lastNewOrder = curNewOrder;
            lastPayment = curPayment;
            stmt = stmtVodkaTime.getStmtUpdateVodkaTime();
            try {
                Thread.sleep(1000);
                stmt.setLong(1, deltaNewOrder);
                stmt.setLong(2, deltaPayment);
                stmt.executeUpdate();
            } catch (InterruptedException | SQLException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void addNeworder(int num) {
        newOrder.addAndGet(num);
    }

    public void addPayment(int num) {
        payment.addAndGet(num);
    }

    public void close() throws Throwable {
        isFinished = true;
        thread.interrupt();
    }

}
