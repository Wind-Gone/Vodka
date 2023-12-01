package benchmark.olap.query;

import config.CommonConfig;

import java.text.ParseException;

import static config.CommonConfig.DB_OCEANBASE;

public class Q18 extends baseQuery {
    public double k;
    public double b;
    private int dbType;

    public Q18(int dbType) throws ParseException {
        super();
        this.filterRate = benchmark.olap.OLAPClient.filterRate[17];
        this.dbType = dbType;
        this.q = getQuery();
    }

    public String updateQuery() {
        return this.q;
    }

    @Override
    public String getQuery() {
        String query;
        switch (this.dbType) {
            case CommonConfig.DB_TIDB:
                query = "select /*+ read_from_storage(tiflash[vodka_customer, vodka_oorder, vodka_order_line]) */" +
                        "    c_last, " +
                        "    c_w_id,c_d_id,c_id, " +
                        "    o_w_id,o_d_id,o_id, " +
                        "    o_entry_d, " +
                        "    sum(ol_quantity)  AS o_totalprice " +
                        "from " +
                        "    vodka_customer, " +
                        "    vodka_oorder, " +
                        "    vodka_order_line " +
                        "where " +
                        "        (o_w_id,o_d_id,o_id) in ( " +
                        "        select /*+ read_from_storage(tiflash[vodka_order_line]) */" +
                        "            ol_w_id,ol_d_id,ol_o_id " +
                        "        from " +
                        "            vodka_order_line " +
                        "        group by " +
                        "            ol_w_id,ol_d_id,ol_o_id having " +
                        "                sum(ol_quantity) > 300 " +
                        "    ) " +
                        "  and c_w_id=o_w_id and c_d_id=o_d_id and c_id=o_c_id " +
                        "  and ol_w_id=o_w_id and ol_d_id=o_d_id and ol_o_id=o_id " +
                        "group by " +
                        "    c_last, " +
                        "    c_w_id,c_d_id,c_id, " +
                        "    o_w_id,o_d_id,o_id, " +
                        "    o_entry_d " +
                        "order by " +
                        "   o_totalprice DESC, " +
                        "   o_entry_d " +
                        " limit 50;";
                break;
            default:
                query = "select " +
                        "    c_last, " +
                        "    c_w_id,c_d_id,c_id, " +
                        "    o_w_id,o_d_id,o_id, " +
                        "    o_entry_d, " +
                        "    sum(ol_quantity)  AS o_totalprice " +
                        "from " +
                        "    vodka_customer, " +
                        "    vodka_oorder, " +
                        "    vodka_order_line " +
                        "where " +
                        "        (o_w_id,o_d_id,o_id) in ( " +
                        "        select " +
                        "            ol_w_id,ol_d_id,ol_o_id " +
                        "        from " +
                        "            vodka_order_line " +
                        "        group by " +
                        "            ol_w_id,ol_d_id,ol_o_id having " +
                        "                sum(ol_quantity) > 300 " +
                        "    ) " +
                        "  and c_w_id=o_w_id and c_d_id=o_d_id and c_id=o_c_id " +
                        "  and ol_w_id=o_w_id and ol_d_id=o_d_id and ol_o_id=o_id " +
                        "group by " +
                        "    c_last, " +
                        "    c_w_id,c_d_id,c_id, " +
                        "    o_w_id,o_d_id,o_id, " +
                        "    o_entry_d " +
                        "order by " +
                        "   o_totalprice DESC, " +
                        "   o_entry_d " +
                        " limit 50;";
                break;
        }
        return query;
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
        return "";
    }

    @Override
    public String getDetailedExecutionPlan() {
        return "explain (analyze,costs false, timing false, summary false, format json) " + this.q;
    }
}
