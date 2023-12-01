package benchmark.olap.query;

import config.CommonConfig;

import java.text.ParseException;

import static config.CommonConfig.DB_OCEANBASE;

public class Q16 extends baseQuery {
    public double k;
    public double b;
    private int dbType;

    public Q16(int dbType) throws ParseException {
        super();
        this.filterRate = benchmark.olap.OLAPClient.filterRate[15];
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
                query = "select /*+ read_from_storage(tiflash[vodka_stock, vodka_item]) */" +
                        "    i_brand, " +
                        "    i_type, " +
                        "    i_size, " +
                        "    count(distinct s_tocksuppkey) as supplier_cnt " +
                        "from " +
                        "    vodka_stock, " +
                        "    vodka_item " +
                        "where " +
                        "        i_id = s_i_id " +
                        "  and i_brand <> 'Brand#45' " +
                        "  and i_type not like 'MEDIUM POLISHED%' " +
                        "  and i_size in (49, 14, 23, 45, 19, 3, 36, 9) " +
                        "  and s_tocksuppkey not in ( " +
                        "    select /*+ read_from_storage(tiflash[vodka_supplier]) */" +
                        "        s_suppkey " +
                        "    from " +
                        "        vodka_supplier " +
                        "    where " +
                        "            s_comment like '%Customer%Complaints%' " +
                        "            and s_suppkey < 500 " +
                        ") " +
                        "group by " +
                        "    i_brand, " +
                        "    i_type, " +
                        "    i_size " +
                        "order by " +
                        "    supplier_cnt desc, " +
                        "    i_brand, " +
                        "    i_type, " +
                        "    i_size;";
                break;
            default:
                query = "select " +
                        "    i_brand, " +
                        "    i_type, " +
                        "    i_size, " +
                        "    count(distinct s_tocksuppkey) as supplier_cnt " +
                        "from " +
                        "    vodka_stock, " +
                        "    vodka_item " +
                        "where " +
                        "        i_id = s_i_id " +
                        "  and i_brand <> 'Brand#45' " +
                        "  and i_type not like 'MEDIUM POLISHED%' " +
                        "  and i_size in (49, 14, 23, 45, 19, 3, 36, 9) " +
                        "  and s_tocksuppkey not in ( " +
                        "    select " +
                        "        s_suppkey " +
                        "    from " +
                        "        vodka_supplier " +
                        "    where " +
                        "            s_comment like '%Customer%Complaints%' " +
                        "            and s_suppkey < 500 " +
                        ") " +
                        "group by " +
                        "    i_brand, " +
                        "    i_type, " +
                        "    i_size " +
                        "order by " +
                        "    supplier_cnt desc, " +
                        "    i_brand, " +
                        "    i_type, " +
                        "    i_size;";
                break;
        }
        return query;
    }

    // @Override
    // public String getCountQuery() {
    //     String q_str = "";
    //     if (benchmark.olap.OLAPTerminal.countCheck) {
    //         q_str = "select count(*)  " +
    //                 "from " +
    //                 "    vodka_stock, " +
    //                 "    vodka_item " +
    //                 "where " +
    //                 "        i_id = s_i_id " +
    //                 "  and i_brand <> 'Brand#45' " +
    //                 "  and i_type not like 'MEDIUM POLISHED%' " +
    //                 "  and i_size in (49, 14, 23, 45, 19, 3, 36, 9) " +
    //                 "  and s_tocksuppkey not in ( " +
    //                 "    select " +
    //                 "        s_suppkey " +
    //                 "    from " +
    //                 "        vodka_supplier " +
    //                 "    where " +
    //                 "            s_comment like '%Customer%Complaints%' " +
    //                 "            and s_suppkey < 500 " +
    //                 ") ;";
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
