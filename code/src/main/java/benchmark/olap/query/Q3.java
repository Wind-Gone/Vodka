package benchmark.olap.query;

import benchmark.olap.OLAPTerminal;
import benchmark.oltp.OLTPClient;
import config.CommonConfig;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static config.CommonConfig.DB_OCEANBASE;

public class Q3 extends baseQuery {
    private static final Logger log = Logger.getLogger(Q3.class);
    public double k;
    public double b;
    private final int dbType;

    public Q3(int dbType) throws ParseException {
        super();
        this.filterRate = benchmark.olap.OLAPClient.filterRate[2];     //0.0480
        this.dbType = dbType;
        this.k = OLTPClient.k2;
        this.b = OLTPClient.b2;
        this.dynamicParam = getDeltaTimes();
        this.q = getQuery();
    }

    public String updateQuery() throws ParseException {
        this.k = OLTPClient.k2;
        this.b = OLTPClient.b2;
        this.dynamicParam = getDeltaTimes();
        this.q = getQuery();
        return this.q;
    }

    public String getDeltaTimes() throws ParseException {
        double countNumber = OLAPTerminal.orderLineTableSize.get() * filterRate;

        SimpleDateFormat simFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date start_d = simFormat.parse("1998-08-12 00:00:00");//min date 1992-01-01,1995-03-15
        Date start_d_1 = simFormat.parse("1992-01-11 00:00:00");
        if (countNumber > OLAPTerminal.orderlineTableNotNullSize.get()) {
            int s = (int) (((countNumber - this.b) / this.k) / 1000);
            return simFormat.format(super.getDateAfter(start_d, s));
        } else {
            int s = (int) ((countNumber / OLAPTerminal.orderlineTableNotNullSize.get()) * ((2405 + OLTPClient.deltaDays2) * 24 * 60 * 60));
            return simFormat.format(super.getDateAfter(start_d_1, s));
        }
    }

    @Override
    public String getQuery() throws ParseException {
        String query;
        if (this.dbType == CommonConfig.DB_TIDB) {
            query = "select /*+ read_from_storage(tiflash[vodka_customer, vodka_oorder, vodka_order_line]) */" +
                    "    ol_w_id,ol_d_id,ol_o_id, " +
                    "    sum(ol_amount * (1 - ol_discount)) as revenue, " +
                    "    o_entry_d, " +
                    "    o_shippriority " +
                    "from " +
                    "    vodka_customer, " +
                    "    vodka_order_line, " +
                    "    vodka_oorder " +
                    "where " +
                    "        c_mktsegment = 'BUILDING' " +
                    "  and c_w_id = o_w_id and c_d_id = o_d_id and c_id = o_c_id " +
                    "  and ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id " +
                    "  and ol_delivery_d >  '" + this.dynamicParam + "' " +
                    "group by " +
                    "    ol_w_id,ol_d_id,ol_o_id, " +
                    "    o_entry_d, " +
                    "    o_shippriority " +
                    "order by " +
                    "    revenue desc, " +
                    "    o_entry_d limit 10;";
        } else {
            query = "select " +
                    "    ol_w_id,ol_d_id,ol_o_id, " +
                    "    sum(ol_amount * (1 - ol_discount)) as revenue, " +
                    "    o_entry_d, " +
                    "    o_shippriority " +
                    "from " +
                    "    vodka_customer, " +
                    "    vodka_order_line, " +
                    "    vodka_oorder " +
                    "where " +
                    "        c_mktsegment = 'BUILDING' " +
                    "  and c_w_id = o_w_id and c_d_id = o_d_id and c_id = o_c_id " +
                    "  and ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id " +
                    "  and ol_delivery_d > TIMESTAMP '" + this.dynamicParam + "' " +
                    "group by " +
                    "    ol_w_id,ol_d_id,ol_o_id, " +
                    "    o_entry_d, " +
                    "    o_shippriority " +
                    "order by " +
                    "    revenue desc, " +
                    "    o_entry_d limit 10;";
        }
        return query;
    }

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
                    "(select count(*) from vodka_order_line where ol_delivery_d > TIMESTAMP '" + this.dynamicParam + "') " +
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
