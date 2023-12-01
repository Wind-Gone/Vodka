package benchmark.olap.query;

import config.CommonConfig;

import java.text.ParseException;

import static config.CommonConfig.DB_OCEANBASE;

public class Q11 extends baseQuery {
    public double k;
    public double b;
    private int dbType;

    public Q11(int dbType) throws ParseException {
        super();
        this.filterRate = benchmark.olap.OLAPClient.filterRate[10];
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
                query = "select /*+ read_from_storage(tiflash[vodka_stock, vodka_supplier, vodka_nation]) */" +
                        "    s_i_id, " +
                        "    sum(s_supplycost * s_quantity) as value " +
                        "from " +
                        "    vodka_stock, " +
                        "    vodka_supplier, " +
                        "    vodka_nation " +
                        "where " +
                        "    s_tocksuppkey = s_suppkey " +
                        "  and s_nationkey = n_nationkey " +
                        "  and n_name = 'GERMANY' " +
                        "group by " +
                        "    s_i_id having " +
                        "    sum(s_supplycost * s_quantity) > ( " +
                        "    select /*+ read_from_storage(tiflash[vodka_stock, vodka_supplier, vodka_nation]) */" +
                        "    sum(s_supplycost * s_quantity) * 0.0001000000 " +
                        "    from " +
                        "    vodka_stock, " +
                        "    vodka_supplier, " +
                        "    vodka_nation " +
                        "    where " +
                        "    s_tocksuppkey = s_suppkey " +
                        "              and s_nationkey = n_nationkey " +
                        "              and n_name = 'GERMANY' " +
                        "    ) " +
                        "order by " +
                        "    value desc;";
                break;
            default:
                query = "select " +
                        "    s_i_id, " +
                        "    sum(s_supplycost * s_quantity) as value " +
                        "from " +
                        "    vodka_stock, " +
                        "    vodka_supplier, " +
                        "    vodka_nation " +
                        "where " +
                        "    s_tocksuppkey = s_suppkey " +
                        "  and s_nationkey = n_nationkey " +
                        "  and n_name = 'GERMANY' " +
                        "group by " +
                        "    s_i_id having " +
                        "    sum(s_supplycost * s_quantity) > ( " +
                        "    select " +
                        "    sum(s_supplycost * s_quantity) * 0.0001000000 " +
                        "    from " +
                        "    vodka_stock, " +
                        "    vodka_supplier, " +
                        "    vodka_nation " +
                        "    where " +
                        "    s_tocksuppkey = s_suppkey " +
                        "              and s_nationkey = n_nationkey " +
                        "              and n_name = 'GERMANY' " +
                        "    ) " +
                        "order by " +
                        "    value desc;";
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
    //                 "    vodka_stock, " +
    //                 "    vodka_supplier, " +
    //                 "    vodka_nation " +
    //                 "where " +
    //                 "    s_tocksuppkey = s_suppkey " +
    //                 "  and s_nationkey = n_nationkey " +
    //                 "  and n_name = 'GERMANY' ;";
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
