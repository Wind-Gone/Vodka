package benchmark.olap;

import bean.OrderLine;
import bean.ReservoirSamplingSingleton;
import benchmark.olap.query.baseQuery;
import config.CommonConfig;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class OLAPClient {
    private static Logger log = Logger.getLogger(OLAPClient.class);
    private static Properties dbProps;
    private static Integer queryNumber = 22;
    private static Integer seed = 2023;

    public static double[] filterRate = { 0.700, 1, 0.3365, 0.2277, 0.3040,
            0.2098, 0.3156, 0.4555, 1, 0.2659,
            1, 0.2054, 1, 0.3857, 0.4214,
            1, 1, 1, 1, 0.2098,
            1, 1, 1 };

    public static void initFilterRatio(String database, Properties dbProps, int dbType) {
        log.info("Initiating filter rate ...");
        try {
            Connection con = DriverManager.getConnection(database, dbProps);
            Statement stmt = con.createStatement();
            if (dbType == CommonConfig.DB_POSTGRES) {
                stmt.execute("SET max_parallel_workers_per_gather = 64;");
            }
            ResultSet result;
            for (int i = 0; i < queryNumber; i++) {
                String filterSQLPath = "filterQuery/" + (i + 1) + ".sql";
                if (dbType == CommonConfig.DB_POSTGRES || dbType == CommonConfig.DB_POLARDB)
                    filterSQLPath = "filterQuery/pg/" + (i + 1) + ".sql";
                if (i == 0 || i == 2 || i == 3 | i == 4 || i == 5 || i == 6 || i == 7 || i == 9 || i == 11 || i == 13
                        || i == 14 || i == 19) {
                    String filterSQLQuery = OLAPClient.readSQL(filterSQLPath);
                    System.out.println("We are executing query: " + filterSQLQuery);
                    result = stmt.executeQuery(filterSQLQuery);
                    if (result.next())
                        filterRate[i] = Double.parseDouble(result.getString(1));
                } else
                    filterRate[i] = 1;
            }
            System.out.println(
                    "We are executing query: select count(*) from vodka_order_line where ol_delivery_d IS NOT NULL;");
            result = stmt.executeQuery("select count(*) from vodka_order_line where ol_delivery_d IS NOT NULL;");
            if (result.next())
                baseQuery.olNotnullSize = Integer.parseInt(result.getString(1));
            System.out.println("We are executing query: select count(*) from vodka_oorder;");
            result = stmt.executeQuery("select count(*) from vodka_oorder;");
            if (result.next())
                baseQuery.orderOriginSize = Integer.parseInt(result.getString(1));
            System.out.println("We are executing query: select count(*) from vodka_order_line;");
            result = stmt.executeQuery("select count(*) from vodka_order_line;");
            if (result.next())
                baseQuery.olOriginSize = Integer.parseInt(result.getString(1));
            // System.out.println(
            //         "We are executing query: select ol_delivery_d, ol_receipdate, ol_commitdate from vodka_order_line where ol_receipdate IS NOT NULL;");
            // result = stmt.executeQuery(
            //         "select ol_delivery_d, ol_receipdate, ol_commitdate from vodka_order_line where ol_receipdate IS NOT NULL;");
            System.out.println("Data Prepare Done.");
            long startClick = System.currentTimeMillis();
            // int index = 0;
            // int batchSize = 1000000; // 设置批量输出的大小
            // while (result.next()) {
            //     Timestamp timestamp1 = Timestamp.valueOf(result.getString(1));
            //     Timestamp timestamp2 = Timestamp.valueOf(result.getString(2));
            //     Timestamp timestamp3 = Timestamp.valueOf(result.getString(3));
            //     OrderLine orderLine = new OrderLine(timestamp1, timestamp2, timestamp3);
            //     ReservoirSamplingSingleton.getInstance().addOrderLine(orderLine);
            //     if (++index % batchSize == 0) {
            //         System.out.println("current sampling index #" + ++index);
            //     }
            // }
            // long endClick = System.currentTimeMillis();
            // System.out.println("Sampling time is: " + Double.parseDouble(Long.toString(endClick - startClick)));
            // OrderLine orderline = ReservoirSamplingSingleton.getInstance().getOrderLine(filterRate[11]);
            // String query12 = "SELECT COUNT(*) FILTER (where ol_receipdate <  TIMESTAMP '" + orderline.ol_receipdate
            //         + "'" + ")::NUMERIC / COUNT(*) FROM vodka_order_line where ol_receipdate IS NOT NULL;";
            // result = stmt.executeQuery(query12);
            // if (result.next())
            //     System.out.println("start size is" + filterRate[11] + "selectivity size is:" + result.getString(1));
            result.close();
            stmt.close();
            con.close();
            System.out.println("Print Filter Rate Array Value: ");
            for (double rate : filterRate) {
                System.out.println(rate);
            }
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
        log.info("Filter rate initialization done.");
    }

    static String readSQL(String path) throws IOException {
        String result;
        StringBuilder builder = new StringBuilder();
        File file = new File(".", path);
        InputStreamReader streamReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8);
        BufferedReader bufferedReader = new BufferedReader(streamReader);
        while ((result = bufferedReader.readLine()) != null) {
            builder.append(result);
            builder.append(" ");
        }
        bufferedReader.close();
        return builder.toString();
    }
}
