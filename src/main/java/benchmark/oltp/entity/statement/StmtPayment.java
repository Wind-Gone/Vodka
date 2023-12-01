package benchmark.oltp.entity.statement;/*
 * Copyright (C) 2022, Zirui Hu, Rong Yu, Jinkai Xu, Yao Luo, Qingshuai Wang
 */

import config.CommonConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class StmtPayment extends StmtBasic {
    public PreparedStatement stmtPaymentSelectWarehouse;
    public PreparedStatement stmtPaymentSelectDistrict;
    public PreparedStatement stmtPaymentSelectCustomerListByLast;
    public PreparedStatement stmtPaymentSelectCustomer;
    public PreparedStatement stmtPaymentSelectCustomerData;
    public PreparedStatement stmtPaymentUpdateWarehouse;
    public PreparedStatement stmtPaymentUpdateDistrict;
    public PreparedStatement stmtPaymentUpdateCustomer;
    public PreparedStatement stmtPaymentUpdateCustomerWithData;
    public PreparedStatement stmtPaymentInsertHistory;

    public StmtPayment(Connection dbConn, int dbType) throws SQLException {
        // PreparedStatements for PAYMENT
        stmtPaymentSelectWarehouse = dbConn.prepareStatement(
                "SELECT w_name, w_street_1, w_street_2, w_city, " +
                        "       w_state, w_zip " +
                        "    FROM vodka_warehouse " +
                        "    WHERE w_id = ? ");
        stmtPaymentSelectDistrict = dbConn.prepareStatement(
                "SELECT d_name, d_street_1, d_street_2, d_city, " +
                        "       d_state, d_zip " +
                        "    FROM vodka_district " +
                        "    WHERE d_w_id = ? AND d_id = ?");
        stmtPaymentSelectCustomerListByLast = dbConn.prepareStatement(
                "SELECT c_id " +
                        "    FROM vodka_customer " +
                        "    WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? " +
                        "    ORDER BY c_first");
        stmtPaymentSelectCustomer = dbConn.prepareStatement(
                "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, " +
                        "       c_city, c_nationkey, c_zip, c_phone, c_since, c_credit, " +
                        "       c_credit_lim, c_discount, c_balance " +
                        "    FROM vodka_customer " +
                        "    WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? " +
                        "    FOR UPDATE");
        stmtPaymentSelectCustomerData = dbConn.prepareStatement(
                "SELECT c_data " +
                        "    FROM vodka_customer " +
                        "    WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
        stmtPaymentUpdateWarehouse = dbConn.prepareStatement(
                "UPDATE vodka_warehouse " +
                        "    SET w_ytd = w_ytd + ? " +
                        "    WHERE w_id = ?");
        stmtPaymentUpdateDistrict = dbConn.prepareStatement(
                "UPDATE vodka_district " +
                        "    SET d_ytd = d_ytd + ? " +
                        "    WHERE d_w_id = ? AND d_id = ?");
        stmtPaymentUpdateCustomer = dbConn.prepareStatement(
                "UPDATE vodka_customer " +
                        "    SET c_balance = c_balance - ?, " +
                        "        c_ytd_payment = c_ytd_payment + ?, " +
                        "        c_payment_cnt = c_payment_cnt + 1 " +
                        "    WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
        stmtPaymentUpdateCustomerWithData = dbConn.prepareStatement(
                "UPDATE vodka_customer " +
                        "    SET c_balance = c_balance - ?, " +
                        "        c_ytd_payment = c_ytd_payment + ?, " +
                        "        c_payment_cnt = c_payment_cnt + 1, " +
                        "        c_data = ? " +
                        "    WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
        stmtPaymentInsertHistory = dbConn.prepareStatement(
                "INSERT INTO vodka_history (" +
                        "    h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, " +
                        "    h_date, h_amount, h_data) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
    }
}
