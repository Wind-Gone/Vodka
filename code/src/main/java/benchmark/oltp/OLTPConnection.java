package benchmark.oltp;/*
 * jTPCCConnection
 *
 * One connection to the database. Used by either the old style
 * Terminal or the new TimedSUT.
 *
 *
 */

import benchmark.oltp.entity.statement.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class OLTPConnection {
    private StmtNewOrder stmtNewOrder;
    private StmtPayment stmtPayment;
    private StmtOrderStatus stmtOrderStatus;
    private StmtReceiveGoods stmtReceiveGoods;
    private StmtStockLevel stmtStockLevel;
    private StmtDelivery stmtDelivery;
    private StmtVodkaTime stmtVodkaTime;

    public PreparedStatement stmtAbort;
    private Connection dbConn;
    private int dbType;

    public OLTPConnection(Connection dbConn, int dbType)
            throws SQLException {
        this.dbConn = dbConn;
        this.dbType = dbType;
        this.stmtNewOrder = new StmtNewOrder(dbConn, dbType);
        this.stmtPayment = new StmtPayment(dbConn, dbType);
        this.stmtOrderStatus = new StmtOrderStatus(dbConn, dbType);
        this.stmtReceiveGoods = new StmtReceiveGoods(dbConn, dbType);
        this.stmtStockLevel = new StmtStockLevel(dbConn, dbType);
        this.stmtDelivery = new StmtDelivery(dbConn, dbType);
        this.stmtVodkaTime = new StmtVodkaTime(dbConn, dbType);
    }


    public OLTPConnection(String connURL, Properties connProps, int dbType) throws SQLException {
        this(DriverManager.getConnection(connURL, connProps), dbType);
    }

    public void commit() throws SQLException {
        dbConn.commit();
    }

    public void rollback() throws SQLException {
        dbConn.rollback();
    }

    public Connection getConnection() {
        return this.dbConn;
    }

    public StmtNewOrder getStmtNewOrder() {
        return stmtNewOrder;
    }

    public StmtPayment getStmtPayment() {
        return stmtPayment;
    }

    public StmtOrderStatus getStmtOrderStatus() {
        return stmtOrderStatus;
    }

    public StmtReceiveGoods getStmtReceiveGoods() {
        return stmtReceiveGoods;
    }

    public StmtStockLevel getStmtStockLevel() {
        return stmtStockLevel;
    }

    public StmtDelivery getStmtDelivery() {
        return stmtDelivery;
    }

    public PreparedStatement getStmtAbort() {
        return stmtAbort;
    }

    public StmtVodkaTime getStmtVodkaTime() {
        return stmtVodkaTime;
    }
}
