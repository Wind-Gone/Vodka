package utils.common;


import bean.DBType;

public class DBTypeUtil {
    public static DBType getDbType(String db) {
        DBType dbType;
        switch (db) {
            case "postgres" -> dbType = DBType.DB_POSTGRES;
            case "mysql" -> dbType = DBType.DB_MYSQL;
            case "ob" -> dbType = DBType.DB_OCEANBASE;
            case "tidb" -> dbType = DBType.DB_TIDB;
            case "polardb" -> dbType = DBType.DB_POLARDB;
            case "oracle" -> dbType = DBType.DB_ORACLE;
            default -> throw new RuntimeException("get unexpected database type, check input 'db': " + db);
        }
        return dbType;
    }
}
