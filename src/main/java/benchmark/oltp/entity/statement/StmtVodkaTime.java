package benchmark.oltp.entity.statement;/*
 * Copyright (C) 2022, Zirui Hu, Rong Yu, Jinkai Xu, Yao Luo, Qingshuai Wang
 */

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class StmtVodkaTime extends StmtBasic {
    public PreparedStatement stmtGetCurrentVodkaTime;
    public PreparedStatement stmtUpdateVodkaTime;

    public StmtVodkaTime(Connection dbConn, int dbType) throws SQLException {
        // PreparedStataments for VODKA_TIME
        stmtGetCurrentVodkaTime = dbConn.prepareStatement(
            "SELECT new_order, payment from vodka_time;"
        );

        stmtUpdateVodkaTime = dbConn.prepareStatement(
                "UPDATE vodka_time " +
                "    SET new_order = new_order + ?, payment = payment + ?");
    }

    public PreparedStatement getStmtGetCurrentVodkaTime() {
        return stmtGetCurrentVodkaTime;
    }

    public PreparedStatement getStmtUpdateVodkaTime() {
        return stmtUpdateVodkaTime;
    }
}

