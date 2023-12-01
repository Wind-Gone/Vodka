package benchmark.oltp.entity.statement;/*
 * Copyright (C) 2022, Zirui Hu, Rong Yu, Jinkai Xu, Yao Luo, Qingshuai Wang
 */

import config.CommonConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class StmtStockLevel extends StmtBasic {
    public PreparedStatement stmtStockLevelSelectLow;
    public PreparedStatement stmtStockLevelStoredProc;
    public String stmtStockLevelStoredProcOracle;

    public StmtStockLevel(Connection dbConn, int dbType) throws SQLException {
        // PreparedStatements for STOCK_LEVEL
        if (dbType == CommonConfig.DB_POSTGRES) {
            stmtStockLevelSelectLow = dbConn.prepareStatement(
                    "SELECT count(*) AS low_stock FROM (" +
                            "    SELECT s_w_id, s_i_id, s_quantity " +
                            "        FROM vodka_stock " +
                            "        WHERE s_w_id = ? AND s_quantity < ? AND s_i_id IN (" +
                            "            SELECT ol_i_id " +
                            "                FROM vodka_district " +
                            "                JOIN vodka_order_line ON ol_w_id = d_w_id " +
                            "                 AND ol_d_id = d_id " +
                            "                 AND ol_o_id >= d_next_o_id - 20 " +
                            "                 AND ol_o_id < d_next_o_id " +
                            "                WHERE d_w_id = ? AND d_id = ? " +
                            "        ) " +
                            "    ) AS L");
        } else if (dbType == CommonConfig.DB_TIDB) {
                stmtStockLevelSelectLow = dbConn.prepareStatement(
                        "SELECT count(*) AS low_stock FROM (" +
                                "    SELECT s_w_id, s_i_id, s_quantity " +
                                "        FROM vodka_stock " +
                                "        WHERE s_w_id = ? AND s_quantity < ? AND s_i_id IN (" +
                                "            SELECT /*+ TIDB_INLJ(vodka_order_line) */ ol_i_id " +
                                "                FROM vodka_district " +
                                "                JOIN vodka_order_line ON ol_w_id = d_w_id " +
                                "                 AND ol_d_id = d_id " +
                                "                 AND ol_o_id >= d_next_o_id - 20 " +
                                "                 AND ol_o_id < d_next_o_id " +
                                "                WHERE d_w_id = ? AND d_id = ? " +
                                "        ) " +
                                "    ) AS L");
            } else {
            stmtStockLevelSelectLow = dbConn.prepareStatement(
                    "SELECT count(*) AS low_stock FROM (" +
                            "    SELECT s_w_id, s_i_id, s_quantity " +
                            "        FROM vodka_stock " +
                            "        WHERE s_w_id = ? AND s_quantity < ? AND s_i_id IN (" +
                            "            SELECT ol_i_id " +
                            "                FROM vodka_district " +
                            "                JOIN vodka_order_line ON ol_w_id = d_w_id " +
                            "                 AND ol_d_id = d_id " +
                            "                 AND ol_o_id >= d_next_o_id - 20 " +
                            "                 AND ol_o_id < d_next_o_id " +
                            "                WHERE d_w_id = ? AND d_id = ? " +
                            "        ) " +
                            "    )");
        }

        switch (dbType) {
            case CommonConfig.DB_POSTGRES -> stmtStockLevelStoredProc = dbConn.prepareStatement(
                    "SELECT * FROM vodka_proc_stock_level (?, ?, ?)");
            case CommonConfig.DB_ORACLE -> stmtStockLevelStoredProcOracle =
                    "{call tpccc_oracle.oracle_proc_stock_level(?, ?, ?, ?)}";
        }
    }

    public PreparedStatement getStmtStockLevelSelectLow() {
        return stmtStockLevelSelectLow;
    }

    public PreparedStatement getStmtStockLevelStoredProc() {
        return stmtStockLevelStoredProc;
    }

    public String getStmtStockLevelStoredProcOracle() {
        return stmtStockLevelStoredProcOracle;
    }
}
