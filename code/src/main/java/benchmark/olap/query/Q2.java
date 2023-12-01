package benchmark.olap.query;

import config.CommonConfig;

import java.text.ParseException;

import static config.CommonConfig.DB_OCEANBASE;

public class Q2 extends baseQuery {
    //    public double k;
//    public double b;
    private int dbType;

    public Q2(int dbType) throws ParseException {
        super();
        this.filterRate = benchmark.olap.OLAPClient.filterRate[1];
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
        switch (dbType) {
            case CommonConfig.DB_TIDB:
                query = "select /*+ read_from_storage(tiflash[vodka_item, vodka_supplier, vodka_stock, vodka_nation, vodka_region]) */" +
                        "    s_acctbal, " +
                        "    s_name, " +
                        "    n_name, " +
                        "    i_id, " +
                        "    i_mfgr, " +
                        "    s_address, " +
                        "    s_phone, " +
                        "    s_comment " +
                        "from " +
                        "    vodka_item, " +
                        "    vodka_supplier, " +
                        "    vodka_stock, " +
                        "    vodka_nation, " +
                        "    vodka_region " +
                        "where " +
                        "        i_id = s_i_id " +
                        "  and s_suppkey = s_tocksuppkey " +
                        "  and i_size = 15 " +
                        "  and i_type like '%BRASS' " +
                        "  and s_nationkey = n_nationkey " +
                        "  and n_regionkey = r_regionkey " +
                        "  and r_name = 'EUROPE' " +
                        "  and s_supplycost = ( " +
                        "    select /*+ read_from_storage(tiflash[vodka_stock, vodka_supplier, vodka_nation, vodka_region]) */" +
                        "        min(s_supplycost) " +
                        "    from " +
                        "        vodka_stock, " +
                        "        vodka_supplier, " +
                        "        vodka_nation, " +
                        "        vodka_region " +
                        "    where " +
                        "            i_id = s_i_id " +
                        "      and s_suppkey = s_tocksuppkey " +
                        "      and s_nationkey = n_nationkey " +
                        "      and n_regionkey = r_regionkey " +
                        "      and r_name = 'EUROPE' " +
                        ") " +
                        "order by " +
                        "    s_acctbal desc, " +
                        "    n_name, " +
                        "    s_name, " +
                        "    i_id limit 100; ";
                break;
            default:
                query = "select " +
                        "    s_acctbal, " +
                        "    s_name, " +
                        "    n_name, " +
                        "    i_id, " +
                        "    i_mfgr, " +
                        "    s_address, " +
                        "    s_phone, " +
                        "    s_comment " +
                        "from " +
                        "    vodka_item, " +
                        "    vodka_supplier, " +
                        "    vodka_stock, " +
                        "    vodka_nation, " +
                        "    vodka_region " +
                        "where " +
                        "        i_id = s_i_id " +
                        "  and s_suppkey = s_tocksuppkey " +
                        "  and i_size = 15 " +
                        "  and i_type like '%BRASS' " +
                        "  and s_nationkey = n_nationkey " +
                        "  and n_regionkey = r_regionkey " +
                        "  and r_name = 'EUROPE' " +
                        "  and s_supplycost = ( " +
                        "    select " +
                        "        min(s_supplycost) " +
                        "    from " +
                        "        vodka_stock, " +
                        "        vodka_supplier, " +
                        "        vodka_nation, " +
                        "        vodka_region " +
                        "    where " +
                        "            i_id = s_i_id " +
                        "      and s_suppkey = s_tocksuppkey " +
                        "      and s_nationkey = n_nationkey " +
                        "      and n_regionkey = r_regionkey " +
                        "      and r_name = 'EUROPE' " +
                        ") " +
                        "order by " +
                        "    s_acctbal desc, " +
                        "    n_name, " +
                        "    s_name, " +
                        "    i_id limit 100; ";
                break;
        }
        // String explain_query = "explain analyse " + query;
        return query;
    }

    // @Override
    // public String getCountQuery() {
    //     String q_str = "";
    //     if (benchmark.olap.OLAPTerminal.countCheck) {
    //         q_str = "select  count(*) " +
    //                 "from " +
    //                 "    vodka_item, " +
    //                 "    vodka_supplier, " +
    //                 "    vodka_stock, " +
    //                 "    vodka_nation, " +
    //                 "    vodka_region " +
    //                 "where " +
    //                 "        i_id = s_i_id " +
    //                 "  and s_suppkey = s_tocksuppkey " +
    //                 "  and i_size = 15 " +
    //                 "  and i_type like '%BRASS' " +
    //                 "  and s_nationkey = n_nationkey " +
    //                 "  and n_regionkey = r_regionkey " +
    //                 "  and r_name = 'EUROPE' " +
    //                 "  and s_supplycost = ( " +
    //                 "    select " +
    //                 "        min(s_supplycost) " +
    //                 "    from " +
    //                 "        vodka_stock, " +
    //                 "        vodka_supplier, " +
    //                 "        vodka_nation, " +
    //                 "        vodka_region " +
    //                 "    where " +
    //                 "            i_id = s_i_id " +
    //                 "      and s_suppkey = s_tocksuppkey " +
    //                 "      and s_nationkey = n_nationkey " +
    //                 "      and n_regionkey = r_regionkey " +
    //                 "      and r_name = 'EUROPE' " +
    //                 ") ; ";
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
