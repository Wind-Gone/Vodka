package benchmark.olap.query;

import benchmark.olap.OLAPTerminal;
import benchmark.oltp.OLTPClient;
import config.CommonConfig;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static config.CommonConfig.DB_OCEANBASE;

public class Q5 extends baseQuery {
    private static Logger log = Logger.getLogger(Q5.class);
    public double k;
    public double b;
    private int dbType;

    public Q5(int dbType) throws ParseException {
        super();
        // this.k = OLTPClient.k1;
        // this.b = OLTPClient.b1;
        this.filterRate = benchmark.olap.OLAPClient.filterRate[4];  //o_entry_d=0.1534
        this.dbType = dbType;
        this.dynamicParam = getDeltaTimes();
        this.q = getQuery();
    }

    public String updateQuery() throws ParseException {
//        this.orderlineTSize=OLAPTerminal.orderLineTableSize;
//        this.orderTSize=OLAPTerminal.oorderTableSize;
//        this.olNotnullSize= OLAPTerminal.orderlineTableNotNullSize;
        this.k = OLTPClient.k1;
        this.b = OLTPClient.b1;
        this.dynamicParam = getDeltaTimes();
        this.q = getQuery();
        return this.q;
    }

    public String getDeltaTimes() throws ParseException {
        double countNumber = OLAPTerminal.oorderTableSize.get() * filterRate;
//        log.info("#5:filterRate" + filterRate);
        // log.info("#1:countNumber");      

        SimpleDateFormat simFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start_d = simFormat.parse("1998-08-02 00:00:00");//min date 1992-01-01,1994-01-01
        Date start_d_1 = simFormat.parse("1992-01-01 00:00:00");
//        log.info("#5: countNumber:" + countNumber + ",size:" + this.orderOriginSize);

        if (countNumber >= this.orderOriginSize) {
            // int s = (int) (((countNumber - this.b) / this.k) / 1000);
            // log.info("Q5-this.b" + this.b + ",this.k" + this.k);
            // log.info("Q5-1 s: " + s);
            // return simFormat.format(super.getDateAfter(start_d, s));
            int s = (int) ((countNumber / OLAPTerminal.oorderTableSize.get()) * ((2405+OLTPClient.deltaDays) * 24 * 60 * 60));
//            log.info("Q5-1 s: " + s);
            return simFormat.format(super.getDateAfter(start_d_1, s));
        } else {
            // double historyNumber = this.orderOriginSize - countNumber;
            int s = (int) ((countNumber / this.orderOriginSize) * (2405 * 24 * 60 * 60));
//            log.info("Q5-2 s: " + s);
            return simFormat.format(super.getDateAfter(start_d_1, s));
        }
    }

    @Override
    public String getQuery() throws ParseException {
        // this.dynamicParam = getDeltaTimes();
        String query;
        switch (dbType) {
            case CommonConfig.DB_TIDB:
                query = "select /*+ read_from_storage(tiflash[vodka_customer, vodka_oorder, vodka_order_line, vodka_supplier, vodka_nation, vodka_region]) */" +
                        "    n_name, " +
                        "    sum(ol_amount * (1 - ol_discount)) as revenue " +
                        "from " +
                        "    vodka_customer, " +
                        "    vodka_oorder, " +
                        "    vodka_order_line, " +
                        "    vodka_supplier, " +
                        "    vodka_nation, " +
                        "    vodka_region " +
                        "where " +
                        "        c_w_id = o_w_id and c_d_id = o_d_id and c_id = o_c_id " +
                        "  and ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id " +
                        "  and ol_suppkey = s_suppkey " +
                        "  and c_nationkey = s_nationkey " +
                        "  and s_nationkey = n_nationkey " +
                        "  and n_regionkey = r_regionkey " +
                        "  and r_name= 'ASIA' " +
                        "  and o_entry_d <  '" + this.dynamicParam + "' " +
                        "  and o_entry_d >=  '" + this.dynamicParam + "'  - interval '1' year " +
                        "group by " +
                        "    n_name " +
                        "order by " +
                        "    revenue desc;";
                break;
            default:
                query = "select " +
                        "    n_name, " +
                        "    sum(ol_amount * (1 - ol_discount)) as revenue " +
                        "from " +
                        "    vodka_customer, " +
                        "    vodka_oorder, " +
                        "    vodka_order_line, " +
                        "    vodka_supplier, " +
                        "    vodka_nation, " +
                        "    vodka_region " +
                        "where " +
                        "        c_w_id = o_w_id and c_d_id = o_d_id and c_id = o_c_id " +
                        "  and ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id " +
                        "  and ol_suppkey = s_suppkey " +
                        "  and c_nationkey = s_nationkey " +
                        "  and s_nationkey = n_nationkey " +
                        "  and n_regionkey = r_regionkey " +
                        "  and r_name= 'ASIA' " +
                        "  and o_entry_d < TIMESTAMP '" + this.dynamicParam + "' " +
                        "  and o_entry_d >= TIMESTAMP '" + this.dynamicParam + "'  - interval '1' year " +
                        "group by " +
                        "    n_name " +
                        "order by " +
                        "    revenue desc;";
                break;
        }
        return query;
    }

    // @Override
    // public String getCountQuery() {
    //     String q_str = "";
    //     if (benchmark.olap.OLAPTerminal.countCheck) {
    //         q_str = "select count(*) " +
    //                 "from " +
    //                 "    vodka_oorder " +
    //                 "where " +
    //                 "  o_entry_d >= date '" + this.dynamicParam + "' " +
    //                 "  and o_entry_d < date '" + this.dynamicParam + "'  + interval '1' year ;";
    //     }
    //     return q_str;
    // }

    @Override
    public String getExplainQuery() {
        switch (dbType) {
            case DB_OCEANBASE -> {
                return "EXPLAIN EXTENDED " + this.q;
            }
            default -> {
                return "EXPLAIN ANALYZE " + this.q;
            }
        }
    }

    @Override
    public String getFilterCheckQuery() {
        if (benchmark.olap.OLAPTerminal.filterRateCheck) {
            return "select ( " +
                    "(select count(*) from vodka_oorder where o_entry_d < TIMESTAMP '" + this.dynamicParam + "'  ) " +
                    "/ " +
                    "(select count(*) from vodka_oorder) " +
                    ");";
        }
        return "";
    }

    @Override
    public String getDetailedExecutionPlan() {
        return "explain (analyze,costs false, timing false, summary false, format json) " + this.q;
    }
}
