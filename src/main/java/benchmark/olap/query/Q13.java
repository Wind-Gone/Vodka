package benchmark.olap.query;

import config.CommonConfig;
import org.apache.log4j.Logger;

import java.text.ParseException;

import static config.CommonConfig.DB_OCEANBASE;

public class Q13 extends baseQuery {
    private static Logger log = Logger.getLogger(Q13.class);
    public double k;
    public double b;
    private int dbType;

    public Q13(int idbType) throws ParseException {
        super();
        this.filterRate = benchmark.olap.OLAPClient.filterRate[12];
        this.dbType = idbType;
        // log.info("#1is this use ob? this.dbType:"+this.dbType);
        this.q = this.getQuery();
    }

    public String updateQuery() {
        // this.dynamicParam = getDeltaTimes();
//        this.q = getQuery();
        return this.q;
    }

    @Override
    public String getQuery() {
        String query = "";

        switch (this.dbType) {
            case CommonConfig.DB_OCEANBASE:
                query = "select " +
                        "    c_count, " +
                        "    count(*) as custdist " +
                        "from " +
                        "    ( " +
                        "        select " +
                        "            c_w_id,c_d_id,c_id, " +
                        "            count(*) as c_count " +
                        "        from " +
                        "            vodka_customer left outer join vodka_oorder on " +
                        "                        c_w_id = o_w_id and c_d_id = o_d_id and c_id = o_c_id " +
                        "                    and o_comment not like '%special%requests%' " +
                        "        group by " +
                        "            c_w_id,c_d_id,c_id " +
                        "    ) as c_orders " +
                        "group by " +
                        "    c_count " +
                        "order by " +
                        "    custdist desc, " +
                        "    c_count desc;";
                break;
            case CommonConfig.DB_TIDB:
                query = "select " +
                        "    c_count, " +
                        "    count(*) as custdist " +
                        "from " +
                        "    ( " +
                        "        select /*+ read_from_storage(tiflash[vodka_customer, vodka_oorder]) */" +
                        "            c_w_id,c_d_id,c_id, " +
                        "            count(*) as c_count " +
                        "        from " +
                        "            vodka_customer left outer join vodka_oorder on " +
                        "                        c_w_id = o_w_id and c_d_id = o_d_id and c_id = o_c_id " +
                        "                    and o_comment not like '%special%requests%' " +
                        "        group by " +
                        "            c_w_id,c_d_id,c_id " +
                        "    ) as c_orders " +
                        "group by " +
                        "    c_count " +
                        "order by " +
                        "    custdist desc, " +
                        "    c_count desc;";
                break;
            default: //CommonConfig.DB_POSTGRES:
                query = "select " +
                        "    c_count, " +
                        "    count(*) as custdist " +
                        "from " +
                        "    ( " +
                        "        select " +
                        "            c_w_id,c_d_id,c_id, " +
                        "            count(*) " +
                        "        from " +
                        "            vodka_customer left outer join vodka_oorder on " +
                        "                        c_w_id = o_w_id and c_d_id = o_d_id and c_id = o_c_id " +
                        "                    and o_comment not like '%special%requests%' " +
                        "        group by " +
                        "            c_w_id,c_d_id,c_id " +
                        "    ) as c_orders (c_custkey, c_count) " +
                        "group by " +
                        "    c_count " +
                        "order by " +
                        "    custdist desc, " +
                        "    c_count desc;";
                break;
        }

        // log.info(query);
        // log.info("#2is this use ob? this.dbType:"+this.dbType);
        return query;
    }

    // @Override
    // public String getCountQuery() {
    //     String q_str = "";
    //     switch (this.dbType) {
    //         case CommonConfig.DB_OCEANBASE:
    //             if (benchmark.olap.OLAPTerminal.countCheck) {
    //                 q_str = "select count(*) " +
    //                         "from " +
    //                         "    ( " +
    //                         "        select " +
    //                         "            c_w_id,c_d_id,c_id, " +
    //                         "            count(*) as c_count " +
    //                         "        from " +
    //                         "            vodka_customer left outer join vodka_oorder on " +
    //                         "                        c_w_id = o_w_id and c_d_id = o_d_id and c_id = o_c_id " +
    //                         "                    and o_comment not like '%special%requests%' " +
    //                         "        group by " +
    //                         "            c_w_id,c_d_id,c_id " +
    //                         "    ) as c_orders ;";
    //             }
    //             break;
    //         default: //CommonConfig.DB_POSTGRES:
    //             if (benchmark.olap.OLAPTerminal.countCheck) {
    //                 q_str = "select count(*) " +
    //                         "from " +
    //                         "    ( " +
    //                         "        select " +
    //                         "            c_w_id,c_d_id,c_id, " +
    //                         "            count(*) " +
    //                         "        from " +
    //                         "            vodka_customer left outer join vodka_oorder on " +
    //                         "                        c_w_id = o_w_id and c_d_id = o_d_id and c_id = o_c_id " +
    //                         "                    and o_comment not like '%special%requests%' " +
    //                         "        group by " +
    //                         "            c_w_id,c_d_id,c_id " +
    //                         "    ) as c_orders (c_custkey, c_count);";
    //             }
    //             break;
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
