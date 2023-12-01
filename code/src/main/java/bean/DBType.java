package bean;


public enum DBType {
    DB_POSTGRES("postgres"),
    DB_ORACLE("oracle"),
    DB_MYSQL("mysql"),
    DB_OCEANBASE("oceanbase"),
    DB_TIDB("tidb"),
    DB_POLARDB("polardb"),
    DB_UNKNOWN("unknown");

    private final String name;

    DBType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
