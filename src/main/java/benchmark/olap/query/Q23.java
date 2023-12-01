package benchmark.olap.query;

import config.CommonConfig;

import java.text.ParseException;

import static config.CommonConfig.DB_OCEANBASE;

public class Q23 extends baseQuery {
    public double k;
    public double b;
    private int dbType;

    public Q23(int dbType) throws ParseException {
        super();
        this.filterRate = benchmark.olap.OLAPClient.filterRate[22];
        this.dbType = dbType;
        this.q = getQuery();
    }

    public String updateQuery() {
        // this.dynamicParam = getDeltaTimes();
        return this.q;
    }

    //  根据历史订单找到某个地区的潜在重要客户
    @Override
    public String getQuery() {
        String query;
        switch (this.dbType) {
            case CommonConfig.DB_TIDB:
                query =
                        "select /*+ read_from_storage(tiflash[vodka_history, vodka_customer, vodka_nation, vodka_region]) */ c_first, c_last, sum(h_amount) as sum_amount, c_balance " +
                                "from " +
                                "vodka_history, " +
                                "vodka_customer, " +
                                "vodka_nation, " +
                                "vodka_region " +
                                "where " +
                                "vodka_history.h_c_id = vodka_customer.c_id " +
                                "and " +
                                "vodka_history.h_c_w_id = vodka_customer.c_w_id " +
                                "and " +
                                "vodka_history.h_c_d_id = vodka_customer.c_d_id " +
                                "and " +
                                "c_nationkey = n_nationkey " +
                                "and " +
                                "n_regionkey = r_regionkey " +
                                "and " +
                                "r_name = 'EUROPE' " +
                                " group by " +
                                "c_id,c_w_id,c_d_id " +
                                "order by " +
                                "sum_amount DESC, c_balance ASC;";
                break;
            default:
                query =
                        "select c_first, c_last, sum(h_amount) as sum_amount, c_balance " +
                                "from " +
                                "vodka_history, " +
                                "vodka_customer, " +
                                "vodka_nation, " +
                                "vodka_region " +
                                "where " +
                                "vodka_history.h_c_id = vodka_customer.c_id " +
                                "and " +
                                "vodka_history.h_c_w_id = vodka_customer.c_w_id " +
                                "and " +
                                "vodka_history.h_c_d_id = vodka_customer.c_d_id " +
                                "and " +
                                "c_nationkey = n_nationkey " +
                                "and " +
                                "n_regionkey = r_regionkey " +
                                "and " +
                                "r_name = 'EUROPE' " +
                                " group by " +
                                "c_id,c_w_id,c_d_id " +
                                "order by " +
                                "sum_amount DESC, c_balance ASC;";
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
        return ";";
    }

    @Override
    public String getDetailedExecutionPlan() {
        return "explain (analyze,costs false, timing false, summary false, format json) " + this.q;
    }
}
