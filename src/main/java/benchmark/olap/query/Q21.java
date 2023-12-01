package benchmark.olap.query;

import config.CommonConfig;

import java.text.ParseException;

import static config.CommonConfig.DB_OCEANBASE;

public class Q21 extends baseQuery {
    public double k;
    public double b;
    private int dbType;

    public Q21(int dbType) throws ParseException {
        super();
        this.filterRate = benchmark.olap.OLAPClient.filterRate[20];
        this.dbType = dbType;
        this.q = getQuery();
    }

    public String updateQuery() {
        // this.dynamicParam = getDeltaTimes();
//        this.q = getQuery();
        return this.q;
    }

    @Override
    public String getQuery() {
        String query;
        switch (this.dbType) {
            case CommonConfig.DB_TIDB:
                query = "select /*+ read_from_storage(tiflash[vodka_supplier, vodka_oorder, vodka_order_line, vodka_nation]) */" +
                        "    s_name, " +
                        "    count(*) as numwait " +
                        "from " +
                        "    vodka_supplier, " +
                        "    vodka_order_line l1, " +
                        "    vodka_oorder, " +
                        "    vodka_nation " +
                        "where " +
                        "        s_suppkey = l1.ol_suppkey " +
                        "  and l1.ol_w_id=o_w_id and l1.ol_d_id=o_d_id and l1.ol_o_id=o_id " +
                        "  and l1.ol_receipdate > l1.ol_commitdate " +
                        "  and exists ( " +
                        "        select /*+ read_from_storage(tiflash[vodka_order_line]) */" +
                        "            * " +
                        "        from " +
                        "            vodka_order_line l2 " +
                        "        where " +
                        "                l2.ol_w_id=l1.ol_w_id and l2.ol_d_id=l1.ol_w_id and l2.ol_o_id=l1.ol_o_id " +
                        "          and l2.ol_suppkey <> l1.ol_suppkey " +
                        "    ) " +
                        "  and not exists ( " +
                        "        select /*+ read_from_storage(tiflash[vodka_order_line]) */" +
                        "            * " +
                        "        from " +
                        "            vodka_order_line l3 " +
                        "        where " +
                        "                l3.ol_w_id=l1.ol_w_id and l3.ol_d_id=l1.ol_w_id and l3.ol_o_id=l1.ol_o_id " +
                        "          and l3.ol_suppkey <> l1.ol_suppkey " +
                        "          and l3.ol_receipdate > l3.ol_commitdate " +
                        "    ) " +
                        "  and s_nationkey = n_nationkey " +
                        "  and n_name = 'SAUDI ARABIA' " +
                        "group by " +
                        "    s_name " +
                        "order by " +
                        "    numwait desc, " +
                        "    s_name limit 100;";
                break;
            default:
                query = "select " +
                        "    s_name, " +
                        "    count(*) as numwait " +
                        "from " +
                        "    vodka_supplier, " +
                        "    vodka_order_line l1, " +
                        "    vodka_oorder, " +
                        "    vodka_nation " +
                        "where " +
                        "        s_suppkey = l1.ol_suppkey " +
                        "  and l1.ol_w_id=o_w_id and l1.ol_d_id=o_d_id and l1.ol_o_id=o_id " +
                        "  and l1.ol_receipdate > l1.ol_commitdate " +
                        "  and exists ( " +
                        "        select " +
                        "            * " +
                        "        from " +
                        "            vodka_order_line l2 " +
                        "        where " +
                        "                l2.ol_w_id=l1.ol_w_id and l2.ol_d_id=l1.ol_w_id and l2.ol_o_id=l1.ol_o_id " +
                        "          and l2.ol_suppkey <> l1.ol_suppkey " +
                        "    ) " +
                        "  and not exists ( " +
                        "        select " +
                        "            * " +
                        "        from " +
                        "            vodka_order_line l3 " +
                        "        where " +
                        "                l3.ol_w_id=l1.ol_w_id and l3.ol_d_id=l1.ol_w_id and l3.ol_o_id=l1.ol_o_id " +
                        "          and l3.ol_suppkey <> l1.ol_suppkey " +
                        "          and l3.ol_receipdate > l3.ol_commitdate " +
                        "    ) " +
                        "  and s_nationkey = n_nationkey " +
                        "  and n_name = 'SAUDI ARABIA' " +
                        "group by " +
                        "    s_name " +
                        "order by " +
                        "    numwait desc, " +
                        "    s_name limit 100;";
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
    //                 "    vodka_supplier, " +
    //                 "    vodka_order_line l1, " +
    //                 "    vodka_oorder, " +
    //                 "    vodka_nation " +
    //                 "where " +
    //                 "        s_suppkey = l1.ol_suppkey " +
    //                 "  and l1.ol_w_id=o_w_id and l1.ol_d_id=o_d_id and l1.ol_o_id=o_id " +
    //                 "  and l1.ol_receipdate > l1.ol_commitdate " +
    //                 "  and exists ( " +
    //                 "        select " +
    //                 "            * " +
    //                 "        from " +
    //                 "            vodka_order_line l2 " +
    //                 "        where " +
    //                 "                l2.ol_w_id=l1.ol_w_id and l2.ol_d_id=l1.ol_w_id and l2.ol_o_id=l1.ol_o_id " +
    //                 "          and l2.ol_suppkey <> l1.ol_suppkey " +
    //                 "    ) " +
    //                 "  and not exists ( " +
    //                 "        select " +
    //                 "            * " +
    //                 "        from " +
    //                 "            vodka_order_line l3 " +
    //                 "        where " +
    //                 "                l3.ol_w_id=l1.ol_w_id and l3.ol_d_id=l1.ol_w_id and l3.ol_o_id=l1.ol_o_id " +
    //                 "          and l3.ol_suppkey <> l1.ol_suppkey " +
    //                 "          and l3.ol_receipdate > l3.ol_commitdate " +
    //                 "    ) " +
    //                 "  and s_nationkey = n_nationkey " +
    //                 "  and n_name = 'SAUDI ARABIA'; ";
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
        return "";
    }

    @Override
    public String getDetailedExecutionPlan() {
        return "explain (analyze,costs false, timing false, summary false, format json) " + this.q;
    }
}
