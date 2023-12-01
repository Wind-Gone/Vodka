package utils.jdbc;/*
 * ExecJDBC - Command line program to process SQL DDL statements, from
 *             a text input file, to any JDBC Data Source
 *
 */


import java.io.*;
import java.sql.*;
import java.util.*;

import utils.exception.SQLExceptionInfo;
import org.apache.log4j.Logger;
import utils.common.jTPCCUtil;

public class ExecJDBC {

    private static final Logger log = Logger.getLogger(ExecJDBC.class);

    public static void main(String[] args) {

        Connection conn = null;
        Statement stmt;
        String rLine;
        String sLine = null;
        StringBuilder sql = new StringBuilder();
        try {
            Properties ini = new Properties();
            ini.load(new FileInputStream(System.getProperty("prop")));
            Class.forName(ini.getProperty("driver"));                                 // Register jdbcDriver
            conn = DriverManager.getConnection(ini.getProperty("conn"),
                    ini.getProperty("user"), ini.getProperty("password"));            // make connection
            conn.setAutoCommit(true);
            String dbType = ini.getProperty("db");                                    // Retrieve datbase type
            boolean ora_ready_to_execute = false;                                     // For oracle : Boolean that indicates whether or not there is a entity.statement ready to be executed.
            stmt = conn.createStatement();                                            // Create Statement
            BufferedReader in = new BufferedReader(new FileReader(
                    jTPCCUtil.getSysProp("commandFile", null)));  // Open inputFile
            while ((rLine = in.readLine()) != null) {                                 // loop thru input file and concatenate SQL entity.statement fragments
                if (ora_ready_to_execute) {
                    String query = sql.toString();
                    execJDBC(stmt, query);
                    sql = new StringBuilder();
                    ora_ready_to_execute = false;
                }
                String line = rLine.trim();
                if (line.length() != 0) {
                    if (line.startsWith("--") && !line.startsWith("-- {")) {
                        System.out.println(rLine);  // print comment line
                    } else {
                        if (line.equals("$$")) {
                            sql.append(rLine);
                            sql.append("\n");
                            while ((rLine = in.readLine()) != null) {
                                line = rLine.trim();
                                sql.append(rLine);
                                sql.append("\n");
                                if (line.equals("$$")) {
                                    break;
                                }
                            }
                            continue;
                        }
                        if (line.startsWith("-- {")) {
                            sql.append(rLine);
                            sql.append("\n");
                            while ((rLine = in.readLine()) != null) {
                                line = rLine.trim();
                                sql.append(rLine);
                                sql.append("\n");
                                if (line.startsWith("-- }")) {
                                    ora_ready_to_execute = true;
                                    break;
                                }
                            }
                            continue;
                        }
                        if (line.endsWith("\\;")) {
                            sql.append(rLine.replaceAll("\\\\;", ";"));
                            sql.append("\n");
                        } else {
                            sql.append(line.replaceAll("\\\\;", ";"));
                            if (line.endsWith(";")) {
                                String query = sql.toString();

                                execJDBC(stmt, query.substring(0, query.length() - 1));
                                sql = new StringBuilder();
                            } else {
                                sql.append("\n");
                            }
                        }
                    }
                } //end if
            } //end while
            in.close();

        } catch (IOException | SQLException ie) {
            System.out.println(ie.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            //exit Cleanly
        } finally {
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            } // end finally
        } // end try
    } // end main


    static void execJDBC(Statement stmt, String query) {
        int retryCount = 1;             // retry times
        int retryThreshold = 6;
        while (true) {
            try {
                System.out.println(query + ";");
                stmt.execute(query);
                break;
            } catch (SQLException se) {
                if (retryCount > retryThreshold) {
                    log.error("Fail to execute sql entity.statement after assigned trys, " + query + ",\n" + SQLExceptionInfo.getSQLExceptionInfo(se));
                    System.exit(3);
                } else {
                    retryCount++;
                }
            }
        } // end execJDBCCommand
    } // end ExecJDBC Class
}
