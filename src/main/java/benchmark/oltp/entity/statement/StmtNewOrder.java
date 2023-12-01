package benchmark.oltp.entity.statement;/*
 * Copyright (C) 2022, Zirui Hu, Rong Yu, Jinkai Xu, Yao Luo, Qingshuai Wang
 */

import lombok.Getter;
import lombok.Setter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Setter
@Getter
public class StmtNewOrder extends StmtBasic {
    public PreparedStatement stmtNewOrderSelectWhseCust;
    public PreparedStatement stmtNewOrderSelectDist;
    public PreparedStatement stmtNewOrderUpdateDist;
    public PreparedStatement stmtNewOrderInsertOrder;
    public PreparedStatement stmtNewOrderInsertNewOrder;
    public PreparedStatement stmtNewOrderSelectStock;
    public PreparedStatement stmtNewOrderSelectStockBatch[];
    public PreparedStatement stmtNewOrderSelectItem;
    public PreparedStatement stmtNewOrderSelectItemBatch[];
    public PreparedStatement stmtNewOrderUpdateStock;
    public PreparedStatement stmtNewOrderInsertOrderLine;
    public PreparedStatement stmtTestForDeadLock;


    public StmtNewOrder(Connection dbConn, int dbType) throws SQLException {
        // PreparedStataments for NEW_ORDER
        stmtNewOrderSelectWhseCust = dbConn.prepareStatement(
                "SELECT c_discount, c_last, c_credit, w_tax " +
                        "    FROM vodka_customer " +
                        "    JOIN vodka_warehouse ON (w_id = c_w_id) " +
                        "    WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
        stmtNewOrderSelectDist = dbConn.prepareStatement(
                "SELECT d_tax, d_next_o_id " +
                        "    FROM vodka_district " +
                        "    WHERE d_w_id = ? AND d_id = ? " +
                        "    FOR UPDATE");
        stmtNewOrderUpdateDist = dbConn.prepareStatement(
                "UPDATE vodka_district " +
                        "    SET d_next_o_id = d_next_o_id + 1 " +
                        "    WHERE d_w_id = ? AND d_id = ?");
        stmtNewOrderInsertOrder = dbConn.prepareStatement(
                "INSERT INTO vodka_oorder (" +
                        "    o_id, o_d_id, o_w_id, o_c_id, o_entry_d, " +
                        "    o_ol_cnt, o_all_local, o_shippriority) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
        stmtNewOrderInsertNewOrder = dbConn.prepareStatement(
                "INSERT INTO vodka_new_order (" +
                        "    no_o_id, no_d_id, no_w_id) " +
                        "VALUES (?, ?, ?)");
        stmtNewOrderSelectStock = dbConn.prepareStatement(
                "SELECT s_quantity, s_data, " +
                        "       s_dist_01, s_dist_02, s_dist_03, s_dist_04, " +
                        "       s_dist_05, s_dist_06, s_dist_07, s_dist_08, " +
                        "       s_dist_09, s_dist_10 " +
                        "    FROM vodka_stock " +
                        "    WHERE s_w_id = ? AND s_i_id = ? " +
                        "    FOR UPDATE");
        stmtNewOrderSelectItem = dbConn.prepareStatement(
                "SELECT i_price, i_name, i_data " +
                        "    FROM vodka_item " +
                        "    WHERE i_id = ?");
        stmtNewOrderUpdateStock = dbConn.prepareStatement(
                "UPDATE vodka_stock " +
                        "    SET s_quantity = ?, s_ytd = s_ytd + ?, " +
                        "        s_order_cnt = s_order_cnt + 1, " +
                        "        s_remote_cnt = s_remote_cnt + ? " +
                        "    WHERE s_w_id = ? AND s_i_id = ?");
        stmtNewOrderInsertOrderLine = dbConn.prepareStatement(
                "INSERT INTO vodka_order_line (" +
                        "    ol_o_id, ol_d_id, ol_w_id, ol_number, " +
                        "    ol_i_id, ol_supply_w_id, ol_quantity, " +
                        "    ol_amount, ol_dist_info, ol_discount, ol_commitdate, ol_suppkey, access_version) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        stmtTestForDeadLock = dbConn.prepareStatement("select * from information_schema.data_lock_waits\\G");
        stmtNewOrderSelectStockBatch = new PreparedStatement[16];
        StringBuilder st = new StringBuilder("SELECT s_i_id, s_w_id, s_quantity, s_data, " +
                "       s_dist_01, s_dist_02, s_dist_03, s_dist_04, " +
                "       s_dist_05, s_dist_06, s_dist_07, s_dist_08, " +
                "       s_dist_09, s_dist_10 " +
                "    FROM vodka_stock " +
                "    WHERE (s_w_id, s_i_id) in ((?,?)");
        for (int i = 1; i <= 15; i++) {
            String stmtStr = st + ") FOR UPDATE";
            stmtNewOrderSelectStockBatch[i] = dbConn.prepareStatement(stmtStr);
            st.append(",(?,?)");
        }
        stmtNewOrderSelectItemBatch = new PreparedStatement[16];
        st = new StringBuilder("SELECT i_id, i_price, i_name, i_data " +
                "    FROM vodka_item WHERE i_id in (?");
        for (int i = 1; i <= 15; i++) {
            String stmtStr = st + ")";
            stmtNewOrderSelectItemBatch[i] = dbConn.prepareStatement(stmtStr);
            st.append(",?");
        }
    }
}