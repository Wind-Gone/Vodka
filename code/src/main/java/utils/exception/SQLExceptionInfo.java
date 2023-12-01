package utils.exception;

import java.io.IOException;
import java.sql.SQLException;

public class SQLExceptionInfo {
    public static String getSQLExceptionInfo(SQLException se) {
        return ", sql state: " + se.getSQLState() +
                ", error code: " + se.getErrorCode() +
                ", message: " + se.getMessage() +
                ", cause: " + se.getCause();
    }

    public static String getIOExceptionInfo(IOException ie) {
        return ", message: " + ie.getMessage() +
                ", cause: " + ie.getCause();
    }
}