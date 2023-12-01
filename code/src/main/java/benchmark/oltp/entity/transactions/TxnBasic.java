package benchmark.oltp.entity.transactions;/*
 * Copyright (C) 2022, Zirui Hu, Rong Yu, Jinkai Xu, Yao Luo, Qingshuai Wang
 */

import java.sql.Connection;
import java.sql.SQLException;

public abstract class TxnBasic {
    private Connection conn;
    private int numWarehouses;
    private int terminalWarehouseID;
    private int terminalDistrictID;
    private boolean useStoredProcedures;
    private int dbType;

    public abstract void generateDate();

    public abstract void execute() throws SQLException;

    public Connection getConn() {
        return conn;
    }

    public void setConn(Connection conn) {
        this.conn = conn;
    }

    public int getNumWarehouses() {
        return numWarehouses;
    }

    public void setNumWarehouses(int numWarehouses) {
        this.numWarehouses = numWarehouses;
    }

    public int getTerminalWarehouse() {
        return terminalWarehouseID;
    }

    public void setTerminalWarehouseID(int terminalWarehouse) {
        this.terminalWarehouseID = terminalWarehouse;
    }

    public int getTerminalDistrictID() {
        return terminalDistrictID;
    }

    public void setTerminalDistrictID(int terminalDistrictID) {
        this.terminalDistrictID = terminalDistrictID;
    }

    public boolean isUseStoredProcedures() {
        return useStoredProcedures;
    }

    public void setUseStoredProcedures(boolean useStoredProcedures) {
        this.useStoredProcedures = useStoredProcedures;
    }

    public int getDbType() {
        return dbType;
    }

    public void setDbType(int dbType) {
        this.dbType = dbType;
    }
}
