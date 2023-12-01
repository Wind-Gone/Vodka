package benchmark.olap.query;

import benchmark.olap.OLAPTerminal;
import benchmark.oltp.OLTPClient;
import config.CommonConfig;
import org.apache.log4j.Logger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static config.CommonConfig.DB_OCEANBASE;

public class Q15 extends baseQuery {
    private static Logger log = Logger.getLogger(Q15.class);
    public double k;
    public double b;
    private final int dbType;

    public Q15(int dbType) throws ParseException {
        super();
        this.filterRate = benchmark.olap.OLAPClient.filterRate[14];
        this.dbType = dbType;
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
        Date start_d = simFormat.parse("1998-08-12 00:00:00");      //min date 1992-01-01,1996-01-01
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
    public String getQuery() {
        return switch (this.dbType) {
            case CommonConfig.DB_TIDB -> "with revenue (supplier_no, total_revenue) as " +
                    "(select /*+ read_from_storage(tiflash[vodka_order_line) */ ol_suppkey , " +
                    "       sum(ol_amount * (1 - ol_discount)) " +
                    "from vodka_order_line " +
                    "where " +
                    "        ol_delivery_d <  '" + this.dynamicParam + "' " +
                    "    and ol_delivery_d >=  '" + this.dynamicParam + "' -  interval '3' month " +
                    "group by ol_suppkey) " +
                    "select s_suppkey, " +
                    "       s_name, " +
                    "       s_address, " +
                    "       s_phone, " +
                    "       total_revenue " +
                    "from /*+ read_from_storage(tiflash[vodka_supplier]) */ vodka_supplier, " +
                    "     revenue " +
                    "where s_suppkey = supplier_no " +
                    "  and total_revenue = (select max(total_revenue) " +
                    "                       from revenue) " +
                    "order by s_suppkey; ";
            case CommonConfig.DB_POSTGRES, CommonConfig.DB_POLARDB -> "with revenue (supplier_no, total_revenue) as " +
                    "(select ol_suppkey , " +
                    "       sum(ol_amount * (1 - ol_discount)) " +
                    "from vodka_order_line " +
                    "where " +
                    "        ol_delivery_d <  TIMESTAMP  '" + this.dynamicParam + "' " +
                    "    and ol_delivery_d >=  TIMESTAMP '" + this.dynamicParam + "' - interval '3' month " +
                    "group by ol_suppkey) " +
                    "select s_suppkey, " +
                    "       s_name, " +
                    "       s_address, " +
                    "       s_phone, " +
                    "       total_revenue " +
                    "from  vodka_supplier, " +
                    "     revenue " +
                    "where s_suppkey = supplier_no " +
                    "  and total_revenue = (select max(total_revenue) " +
                    "                       from revenue) " +
                    "order by s_suppkey;";
            default -> "with revenue (supplier_no, total_revenue) as " +
                    "(select ol_suppkey , " +
                    "       sum(ol_amount * (1 - ol_discount)) " +
                    "from vodka_order_line " +
                    "where " +
                    "        ol_delivery_d <  TIMESTAMP  '" + this.dynamicParam + "' " +
                    "    and ol_delivery_d >=  TIMESTAMP  '" + this.dynamicParam + "' - interval '3' month " +
                    "group by ol_suppkey) " +
                    "select s_suppkey, " +
                    "       s_name, " +
                    "       s_address, " +
                    "       s_phone, " +
                    "       total_revenue " +
                    "from  vodka_supplier, " +
                    "     revenue " +
                    "where s_suppkey = supplier_no " +
                    "  and total_revenue = (select max(total_revenue) " +
                    "                       from revenue) " +
                    "order by s_suppkey; ";
        };
    }

    @Override
    public String getExplainQuery() {
        if (dbType == DB_OCEANBASE) {
            return "EXPLAIN EXTENDED " + this.q;
        }
        return "EXPLAIN ANALYZE " + this.q;
    }

    @Override
    public String getFilterCheckQuery() {
        if (benchmark.olap.OLAPTerminal.filterRateCheck) {
            return "select ( " +
                    "(select count(*) from vodka_order_line where ol_delivery_d < TIMESTAMP '" + this.dynamicParam + "') " +
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
