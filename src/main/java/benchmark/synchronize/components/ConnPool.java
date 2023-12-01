package benchmark.synchronize.components;

import java.util.*;
import java.sql.*;

// database connection pool
public class ConnPool {
    int curSize;
    int maxSize;
    LinkedList<Connection> dbConns;
    String database;
    Properties dbProps;

    public ConnPool(int maxSize, String database, Properties dbProps) {
        this.curSize = 0;
        this.maxSize = maxSize;
        this.database = database;
        this.dbProps = dbProps;
        this.dbConns = new LinkedList<>();
        for (int i = 0; i < maxSize; i++) {
            try {
                Connection conn;
                conn = DriverManager.getConnection(database, dbProps);
                dbConns.addFirst(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public synchronized Connection getConn() {
        // return null;
        Connection conn = null;
        while (conn == null) {
            if (dbConns.size() > 0) {
                conn = dbConns.getLast();
                dbConns.removeLast();
            } else {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
//                System.out.println("getConn wait");
            }
        }
        return conn;
    }

    public synchronized void releaseConn(Connection conn) {
        dbConns.addFirst(conn);
    }

    public void destroyConn(Connection conn) {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        int size = dbConns.size();
        for (Connection dbConn : dbConns) {
            try {
                dbConn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}