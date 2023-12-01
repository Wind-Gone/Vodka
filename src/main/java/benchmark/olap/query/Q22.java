package benchmark.olap.query;

import config.CommonConfig;

import java.text.ParseException;

import static config.CommonConfig.DB_OCEANBASE;

public class Q22 extends baseQuery {
    public double k;
    public double b;
    private int dbType;

    public Q22(int dbType) throws ParseException {
        super();
        this.filterRate = benchmark.olap.OLAPClient.filterRate[21];
        this.dbType = dbType;
        this.q = getQuery();
    }

    public String updateQuery() {
        return this.q;
    }

    @Override
    public String getQuery() {
        String query;
        if (this.dbType == CommonConfig.DB_TIDB) {
            query = "select " +
                    "    cntrycode, " +
                    "    count(*) as numcust, " +
                    "    sum(c_balance) as totacctbal " +
                    "from " +
                    "    ( " +
                    "        select /*+ read_from_storage(tiflash[vodka_customer]) */" +
                    "            substring(c_phone from 1 for 2) as cntrycode, " +
                    "            c_balance " +
                    "        from " +
                    "            vodka_customer " +
                    "        where " +
                    "                substring(c_phone from 1 for 2) in " +
                    "                ('13', '31', '23', '29', '30', '18', '17') " +
                    "          and c_balance > ( " +
                    "            select /*+ read_from_storage(tiflash[vodka_customer]) */" +
                    "                avg(c_balance) " +
                    "            from " +
                    "                vodka_customer " +
                    "            where " +
                    "                    c_balance > 0.00 " +
                    "              and substring(c_phone from 1 for 2) in " +
                    "                  ('13', '31', '23', '29', '30', '18', '17') " +
                    "        ) " +
                    "          and not exists ( " +
                    "                select /*+ read_from_storage(tiflash[vodka_oorder]) */" +
                    "                    * " +
                    "                from " +
                    "                    vodka_oorder " +
                    "                where " +
                    "                        c_w_id=o_w_id and c_d_id=o_d_id and c_id=o_c_id " +
                    "            ) " +
                    "    ) as custsale " +
                    "group by " +
                    "    cntrycode " +
                    "order by " +
                    "    cntrycode;";
        } else {
            query = "select " +
                    "    cntrycode, " +
                    "    count(*) as numcust, " +
                    "    sum(c_balance) as totacctbal " +
                    "from " +
                    "    ( " +
                    "        select " +
                    "            substring(c_phone from 1 for 2) as cntrycode, " +
                    "            c_balance " +
                    "        from " +
                    "            vodka_customer " +
                    "        where " +
                    "                substring(c_phone from 1 for 2) in " +
                    "                ('13', '31', '23', '29', '30', '18', '17') " +
                    "          and c_balance > ( " +
                    "            select " +
                    "                avg(c_balance) " +
                    "            from " +
                    "                vodka_customer " +
                    "            where " +
                    "                    c_balance > 0.00 " +
                    "              and substring(c_phone from 1 for 2) in " +
                    "                  ('13', '31', '23', '29', '30', '18', '17') " +
                    "        ) " +
                    "          and not exists ( " +
                    "                select " +
                    "                    * " +
                    "                from " +
                    "                    vodka_oorder " +
                    "                where " +
                    "                        c_w_id=o_w_id and c_d_id=o_d_id and c_id=o_c_id " +
                    "            ) " +
                    "    ) as custsale " +
                    "group by " +
                    "    cntrycode " +
                    "order by " +
                    "    cntrycode;";
        }
        return query;
    }

    // @Override
    // public String getCountQuery() {
    //     String q_str = "";
    //     if (benchmark.olap.OLAPTerminal.countCheck) {
    //         q_str = "select count(*) " +
    //                 "from " +
    //                 "    ( " +
    //                 "        select " +
    //                 "            substring(c_phone from 1 for 2) as cntrycode, " +
    //                 "            c_balance " +
    //                 "        from " +
    //                 "            vodka_customer " +
    //                 "        where " +
    //                 "                substring(c_phone from 1 for 2) in " +
    //                 "                ('13', '31', '23', '29', '30', '18', '17') " +
    //                 "          and c_balance > ( " +
    //                 "            select " +
    //                 "                avg(c_balance) " +
    //                 "            from " +
    //                 "                vodka_customer " +
    //                 "            where " +
    //                 "                    c_balance > 0.00 " +
    //                 "              and substring(c_phone from 1 for 2) in " +
    //                 "                  ('13', '31', '23', '29', '30', '18', '17') " +
    //                 "        ) " +
    //                 "          and not exists ( " +
    //                 "                select " +
    //                 "                    * " +
    //                 "                from " +
    //                 "                    vodka_oorder " +
    //                 "                where " +
    //                 "                        c_w_id=o_w_id and c_d_id=o_d_id and c_id=o_c_id " +
    //                 "            ) " +
    //                 "    ) as custsale ;";
    //     }
    //     return q_str;
    // }

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
