package config;

import bean.DBType;
import lombok.Getter;
import lombok.Setter;
import utils.common.CheckParamUtil;
import utils.common.DBTypeUtil;

import java.io.IOException;
import java.util.Properties;

@Getter
@Setter
public class ConnectionProperty extends Property {
    private String db;
    private String driver;
    private String conn;
    private String connAP;
    private String host;
    private String port;
    private String settings;
    private String user;
    private String password;
    private String databaseName;

    // jdbc connection parameters
    private Properties dbProps;


    private void checkProperty() throws RuntimeException {
        // check jdbc connection properties with CheckParamsUtil
        CheckParamUtil.checkNull(conn, "conn");
    }

    public ConnectionProperty(String path) throws IOException {
        super(path);
        dbProps = new Properties();
    }

    @Override
    public void loadProperty() throws RuntimeException {
        // database type
        db = props.getProperty("db", null);
        // jdbc connection (necessary)
        host = props.getProperty("host", null);
        port = props.getProperty("port", null);
        driver = props.getProperty("driver", null);
        user = props.getProperty("user", null);
        password = props.getProperty("password", null);
        settings = props.getProperty("settings", null);
        conn = props.getProperty("conn", null);
        databaseName = props.getProperty("databaseName", "benchmarksql");
        connAP = props.getProperty("connAP", null);
//        if (dbProps != null) {
//            dbProps.setProperty("user", user);
//            dbProps.setProperty("password", password);
//            switch (dbType) {
//                // mysql jdbc connection parameters
//                case DB_MYSQL, DB_OCEANBASE, DB_TIDB -> {
//                    conn = String.format("jdbc:mysql://%s:%s/", host, port);
//                    settings = String.format("/%s?%s", databaseName, settings);
//                }
//
//                // postgres jdbc connection parameters
//                case DB_POSTGRES, DB_POLARDB -> {
//                    conn = String.format("jdbc:postgresql://%s:%s/", host, port);
//                    settings = String.format("/%s?%s", databaseName, settings);
//                }
//            }
//        }
        checkProperty();
    }

}
