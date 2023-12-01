package benchmark.olap.query;

import benchmark.olap.OLAPTerminal;
import benchmark.oltp.OLTPClient;
import config.CommonConfig;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static config.CommonConfig.DB_OCEANBASE;

public class Q4 extends baseQuery {
    private static Logger log = Logger.getLogger(Q4.class);
    public double k;
    public double b;
    private int dbType;

    public Q4(int dbType) throws ParseException {
        super();
        this.k = OLTPClient.k1;
        this.b = OLTPClient.b1;
        this.filterRate = benchmark.olap.OLAPClient.filterRate[3];           //o_entry_d
        this.dbType = dbType;
        this.dynamicParam = getDeltaTimes();
        this.q = getQuery();
    }

    public String updateQuery() throws ParseException {
//        this.orderlineTSize=OLAPTerminal.orderLineTableSize;
//        this.orderTSize= OLAPTerminal.oorderTableSize;
//        this.olNotnullSize=OLAPTerminal.orderlineTableNotNullSize;
        this.k = OLTPClient.k1;
        this.b = OLTPClient.b1;
        this.dynamicParam = getDeltaTimes();
        this.q = getQuery();
        return this.q;
    }

    public String getDeltaTimes() throws ParseException {
        double countNumber = OLAPTerminal.oorderTableSize.get() * filterRate;

        SimpleDateFormat simFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //  min date 1992-01-01,1993-07-01
        Date start_d_1 = simFormat.parse("1992-01-01 00:00:00");

        if (countNumber >= this.orderOriginSize) {
            int s = (int) ((countNumber / OLAPTerminal.oorderTableSize.get()) * ((2405 + OLTPClient.deltaDays) * 24 * 60 * 60));
            return simFormat.format(super.getDateAfter(start_d_1, s));
        } else {
            int s = (int) ((countNumber / this.orderOriginSize) * (2405 * 24 * 60 * 60));
            return simFormat.format(super.getDateAfter(start_d_1, s));
        }
    }

    @Override
    public String getQuery() throws ParseException {
        // this.dynamicParam = getDeltaTimes();
        String query;
        if (this.dbType == CommonConfig.DB_TIDB) {
            query = "select /*+ read_from_storage(tiflash[vodka_oorder]) */" +
                    "    o_carrier_id, " +
                    "    count(*) as order_count " +
                    "from " +
                    "    vodka_oorder " +
                    "where " +
                    "        o_entry_d <  '" + this.dynamicParam + "' " +
                    "  and o_entry_d >=  '" + this.dynamicParam + "' - " + " interval '3' month " +
                    "  and exists ( " +
                    "        select /*+ read_from_storage(tiflash[vodka_order_line]) */" +
                    "            * " +
                    "        from " +
                    "            vodka_order_line " +
                    "        where " +
                    "                ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id " +
                    "          and ol_commitdate < ol_receipdate " +
                    "    ) " +
                    "group by " +
                    "    o_carrier_id " +
                    "order by " +
                    "    o_carrier_id;";
        } else {
            query = "select " +
                    "    o_carrier_id, " +
                    "    count(*) as order_count " +
                    "from " +
                    "    vodka_oorder " +
                    "where " +
                    "        o_entry_d < TIMESTAMP '" + this.dynamicParam + "' " +
                    "  and o_entry_d >= TIMESTAMP '" + this.dynamicParam + "' - " + " interval '3' month " +
                    "  and exists ( " +
                    "        select " +
                    "            * " +
                    "        from " +
                    "            vodka_order_line " +
                    "        where " +
                    "                ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id " +
                    "          and ol_commitdate < ol_receipdate " +
                    "    ) " +
                    "group by " +
                    "    o_carrier_id " +
                    "order by " +
                    "    o_carrier_id;";
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
    //                 "        o_entry_d >= date '" + this.dynamicParam + "' " +
    //                 "  and o_entry_d < date '" + this.dynamicParam + "' + " + " interval '3' month ;";
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
                    "(select count(*) from vodka_oorder where o_entry_d < TIMESTAMP '" + this.dynamicParam + "') " +
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
