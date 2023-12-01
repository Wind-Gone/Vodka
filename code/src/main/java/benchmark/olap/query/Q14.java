package benchmark.olap.query;

import benchmark.olap.OLAPTerminal;
import benchmark.oltp.OLTPClient;
import config.CommonConfig;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static benchmark.oltp.OLTPClient.gloabalSysCurrentTime;

import static config.CommonConfig.DB_OCEANBASE;

public class Q14 extends baseQuery {
    private static Logger log = Logger.getLogger(Q14.class);
    public double k;
    public double b;
    private int dbType;

    public Q14(int dbType) throws ParseException {
        super();
        this.k = OLTPClient.k2;
        this.b = OLTPClient.b2;
        this.filterRate = benchmark.olap.OLAPClient.filterRate[13];           //ol_delivery_d=0.0087
        this.dbType = dbType;
        this.dynamicParam = getDeltaTimes();
        this.q = getQuery();
    }

    public String updateQuery() throws ParseException {
//        this.orderlineTSize=OLAPTerminal.orderLineTableSize;
//        this.orderTSize=OLAPTerminal.oorderTableSize;
//        this.olNotnullSize=OLAPTerminal.orderlineTableNotNullSize;
        this.k = OLTPClient.k2;
        this.b = OLTPClient.b2;
        this.dynamicParam = getDeltaTimes();
        this.q = getQuery();
        return this.q;
    }

    public String getDeltaTimes() throws ParseException {
        double countNumber = OLAPTerminal.orderLineTableSize.get() * filterRate;
//        log.info("#14:filterRate" + filterRate);
        // log.info("#1:countNumber");      

        SimpleDateFormat simFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start_d = simFormat.parse("1998-08-12 00:00:00");//min date 1992-01-01,1995-09-01
        Date start_d_1 = simFormat.parse("1992-01-11 00:00:00");
//        log.info("#14 onlineOrderlineTSize:" + OLAPTerminal.orderLineTableSize + ", countNumber:" + countNumber + ",originSize:" + this.olOriginSize + ",notNullSize:" + OLAPTerminal.orderlineTableNotNullSize);
        if (countNumber > OLAPTerminal.orderlineTableNotNullSize.get()) {
            int s = (int) (((countNumber - this.b) / this.k) / 1000);
//            log.info("Q14-this.b" + this.b + ",this.k" + this.k);
//            log.info("Q14-1 s: " + s);
            return simFormat.format(super.getDateAfter(start_d, s));
        } else {
            // double historyNumber = OLAPTerminal.orderlineTableNotNullSize - countNumber;
            int s = (int) ((countNumber / OLAPTerminal.orderlineTableNotNullSize.get()) * ((2405+OLTPClient.deltaDays2) * 24 * 60 * 60));
//            log.info("Q14-2 s: " + s);
            return simFormat.format(super.getDateAfter(start_d_1, s));
        }
    }

    @Override
    public String getQuery() throws ParseException {
        // this.dynamicParam = getDeltaTimes();
        String query;
        switch (this.dbType) {
            case CommonConfig.DB_TIDB:
                query = "select /*+ read_from_storage(tiflash[vodka_order_line, vodka_item]) */" +
                        "            100.00 * sum(case " +
                        "                             when i_type like 'PROMO%' " +
                        "                                 then ol_amount * (1 - ol_discount) " +
                        "                             else 0 " +
                        "            end) / (sum(ol_amount * (1 - ol_discount))+0.001) as promo_revenue " +
                        "from " +
                        "    vodka_order_line, " +
                        "    vodka_item " +
                        "where " +
                        "        ol_i_id = i_id " +
                        "  and ol_delivery_d <  '" + this.dynamicParam + "' " +
                        "  and ol_delivery_d >=  '" + this.dynamicParam + "'  - interval '1' month; ";
                break;
            default:
                query = "select " +
                        "            100.00 * sum(case " +
                        "                             when i_type like 'PROMO%' " +
                        "                                 then ol_amount * (1 - ol_discount) " +
                        "                             else 0 " +
                        "            end) / (sum(ol_amount * (1 - ol_discount))+0.001) as promo_revenue " +
                        "from " +
                        "    vodka_order_line, " +
                        "    vodka_item " +
                        "where " +
                        "        ol_i_id = i_id " +
                        "  and ol_delivery_d < TIMESTAMP  '" + this.dynamicParam + "' " +
                        "  and ol_delivery_d >= TIMESTAMP  '" + this.dynamicParam + "'  - interval '1' month; ";
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
    //                 "    vodka_order_line " +
    //                 "where " +
    //                 "  ol_delivery_d >= date '" + this.dynamicParam + "' " +
    //                 "  and ol_delivery_d < date '" + this.dynamicParam + "'  + interval '1' month; ";
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
                    "(select count(*) from vodka_order_line where ol_delivery_d < TIMESTAMP '" + this.dynamicParam + "' ) " +
                    "/ " +
                    "(select count(*) from vodka_order_line) " +
                    ");";
        }
        return "";
    }

    @Override
    public String getDetailedExecutionPlan() {
        return "explain (analyze,costs false, timing false, summary false, format json) " + this.q;
    }
}
