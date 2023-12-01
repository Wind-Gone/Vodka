package benchmark.olap.query;


import config.CommonConfig;

import java.text.ParseException;

import static config.CommonConfig.DB_OCEANBASE;

public class Q17 extends baseQuery {
    public double k;
    public double b;
    private int dbType;

    public Q17(int dbType) throws ParseException {
        super();
        this.filterRate = benchmark.olap.OLAPClient.filterRate[16];
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
        if (this.dbType == CommonConfig.DB_TIDB) {
            query = "select /*+ read_from_storage(tiflash[vodka_order_line, vodka_item]) */" +
                    "        sum(ol_amount) / 7.0 as avg_yearly " +
                    "from " +
                    "    vodka_order_line, " +
                    "    vodka_item " +
                    "where " +
                    "        i_id = ol_i_id " +
                    "  and i_brand = 'Brand#23' " +
                    "  and i_container = 'MED BOX' " +
                    "  and ol_quantity < (" +
                    "    select /*+ read_from_storage(tiflash[vodka_order_line]) */" +
                    "            0.2 * avg(ol_quantity) " +
                    "    from " +
                    "        vodka_order_line " +
                    "    where " +
                    "            ol_i_id = i_id" +
                    ");";
        } else {
            query = "select " +
                    "        sum(ol_amount) / 7.0 as avg_yearly " +
                    "from " +
                    "    vodka_order_line, " +
                    "    vodka_item " +
                    "where " +
                    "        i_id = ol_i_id " +
                    "  and i_brand = 'Brand#23' " +
                    "  and i_container = 'MED BOX' " +
                    "  and ol_quantity < (" +
                    "    select " +
                    "            0.2 * avg(ol_quantity) " +
                    "    from " +
                    "        vodka_order_line " +
                    "    where " +
                    "            ol_i_id = i_id" +
                    ");";
        }
        return query;
    }

    // @Override
    // public String getCountQuery() {
    //     String q_str = "";
    //     if (benchmark.olap.OLAPTerminal.countCheck) {
    //         q_str = "select count(*) " +
    //                 "from " +
    //                 "    vodka_order_line, " +
    //                 "    vodka_item " +
    //                 "where " +
    //                 "        i_id = ol_i_id " +
    //                 "  and i_brand = 'Brand#23' " +
    //                 "  and i_container = 'MED BOX' " +
    //                 "  and ol_quantity < (" +
    //                 "    select " +
    //                 "            0.2 * avg(ol_quantity) " +
    //                 "    from " +
    //                 "        vodka_order_line " +
    //                 "    where " +
    //                 "            ol_i_id = i_id" +
    //                 ");";
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
