package benchmark.oltp.entity.statement;/*
 * Copyright (C) 2022, Zirui Hu, Rong Yu, Jinkai Xu, Yao Luo, Qingshuai Wang
 */

import config.CommonConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class StmtOrderStatus extends StmtBasic {
    public PreparedStatement stmtOrderStatusSelectCustomerListByLast;
    public PreparedStatement stmtOrderStatusSelectCustomer;
    public PreparedStatement stmtOrderStatusSelectLastOrder;
    public PreparedStatement stmtOrderStatusSelectOrderLine;
    public PreparedStatement stmtOrderStatusStoredProc;
    public String stmtOrderStatusStoredProcOracle;

    public StmtOrderStatus(Connection dbConn, int dbType) throws SQLException {


        // PreparedStatements for ORDER_STATUS
        stmtOrderStatusSelectCustomerListByLast = dbConn.prepareStatement(
                "SELECT c_id " +
                        "    FROM vodka_customer " +
                        "    WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? " +
                        "    ORDER BY c_first");
        stmtOrderStatusSelectCustomer = dbConn.prepareStatement(
                "SELECT c_first, c_middle, c_last, c_balance " +
                        "    FROM vodka_customer " +
                        "    WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
        stmtOrderStatusSelectLastOrder = dbConn.prepareStatement(
                "SELECT o_id, o_entry_d, o_carrier_id " +
                        "    FROM vodka_oorder " +
                        "    WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? " +
                        "      AND o_id = (" +
                        "          SELECT max(o_id) " +
                        "              FROM vodka_oorder " +
                        "              WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?" +
                        "          )");
        stmtOrderStatusSelectOrderLine = dbConn.prepareStatement(
                "SELECT ol_i_id, ol_supply_w_id, ol_quantity, " +
                        "       ol_amount, ol_delivery_d " +
                        "    FROM vodka_order_line " +
                        "    WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? " +
                        "    ORDER BY ol_w_id, ol_d_id, ol_o_id, ol_number");

        switch (dbType) {
            case CommonConfig.DB_POSTGRES -> stmtOrderStatusStoredProc = dbConn.prepareStatement(
                    "SELECT * FROM vodka_proc_order_status (?, ?, ?, ?)");
            case CommonConfig.DB_ORACLE -> stmtOrderStatusStoredProcOracle =
                    "{call tpccc_oracle.oracle_proc_order_status(?, ?, ?, " +
                            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}";
        }
    }
}
