package benchmark.olap.query;


import config.CommonConfig;

import java.text.ParseException;

import static config.CommonConfig.DB_OCEANBASE;

public class Q19 extends baseQuery {
    public double k;
    public double b;
    private int dbType;

    public Q19(int dbType) throws ParseException {
        super();
        this.filterRate = benchmark.olap.OLAPClient.filterRate[18];
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
                query = "select /*+ read_from_storage(tiflash[vodka_item, vodka_order_line]) */" +
                        "    sum(ol_amount* (1 - ol_discount)) as revenue " +
                        "from " +
                        "    vodka_order_line, " +
                        "    vodka_item " +
                        "where " +
                        "    ( " +
                        "                i_id = ol_i_id " +
                        "            and i_brand = 'Brand#12' " +
                        "            and i_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') " +
                        "            and ol_quantity >= 1 and ol_quantity <= 1 + 10 " +
                        "            and i_size between 1 and 5 " +
                        "            and ol_shipmode in ('AIR', 'AIR REG') " +
                        "            and ol_shipinstruct = 'DELIVER IN PERSON' " +
                        "        ) " +
                        "   or " +
                        "    ( " +
                        "                i_id = ol_i_id " +
                        "            and i_brand = 'Brand#23' " +
                        "            and i_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') " +
                        "            and ol_quantity >= 10 and ol_quantity <= 10 + 10 " +
                        "            and i_size between 1 and 10 " +
                        "            and ol_shipmode in ('AIR', 'AIR REG') " +
                        "            and ol_shipinstruct = 'DELIVER IN PERSON' " +
                        "        ) " +
                        "   or " +
                        "    ( " +
                        "                i_id = ol_i_id " +
                        "            and i_brand = 'Brand#34' " +
                        "            and i_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') " +
                        "            and ol_quantity >= 20 and ol_quantity <= 20 + 10 " +
                        "            and i_size between 1 and 15 " +
                        "            and ol_shipmode in ('AIR', 'AIR REG') " +
                        "            and ol_shipinstruct = 'DELIVER IN PERSON' " +
                        "        );";
                break;
            default:
                query = "select " +
                        "    sum(ol_amount* (1 - ol_discount)) as revenue " +
                        "from " +
                        "    vodka_order_line, " +
                        "    vodka_item " +
                        "where " +
                        "    ( " +
                        "                i_id = ol_i_id " +
                        "            and i_brand = 'Brand#12' " +
                        "            and i_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') " +
                        "            and ol_quantity >= 1 and ol_quantity <= 1 + 10 " +
                        "            and i_size between 1 and 5 " +
                        "            and ol_shipmode in ('AIR', 'AIR REG') " +
                        "            and ol_shipinstruct = 'DELIVER IN PERSON' " +
                        "        ) " +
                        "   or " +
                        "    ( " +
                        "                i_id = ol_i_id " +
                        "            and i_brand = 'Brand#23' " +
                        "            and i_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') " +
                        "            and ol_quantity >= 10 and ol_quantity <= 10 + 10 " +
                        "            and i_size between 1 and 10 " +
                        "            and ol_shipmode in ('AIR', 'AIR REG') " +
                        "            and ol_shipinstruct = 'DELIVER IN PERSON' " +
                        "        ) " +
                        "   or " +
                        "    ( " +
                        "                i_id = ol_i_id " +
                        "            and i_brand = 'Brand#34' " +
                        "            and i_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') " +
                        "            and ol_quantity >= 20 and ol_quantity <= 20 + 10 " +
                        "            and i_size between 1 and 15 " +
                        "            and ol_shipmode in ('AIR', 'AIR REG') " +
                        "            and ol_shipinstruct = 'DELIVER IN PERSON' " +
                        "        );";
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
    //                 "    vodka_order_line, " +
    //                 "    vodka_item " +
    //                 "where " +
    //                 "    ( " +
    //                 "                i_id = ol_i_id " +
    //                 "            and i_brand = 'Brand#12' " +
    //                 "            and i_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') " +
    //                 "            and ol_quantity >= 1 and ol_quantity <= 1 + 10 " +
    //                 "            and i_size between 1 and 5 " +
    //                 "            and ol_shipmode in ('AIR', 'AIR REG') " +
    //                 "            and ol_shipinstruct = 'DELIVER IN PERSON' " +
    //                 "        ) " +
    //                 "   or " +
    //                 "    ( " +
    //                 "                i_id = ol_i_id " +
    //                 "            and i_brand = 'Brand#23' " +
    //                 "            and i_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') " +
    //                 "            and ol_quantity >= 10 and ol_quantity <= 10 + 10 " +
    //                 "            and i_size between 1 and 10 " +
    //                 "            and ol_shipmode in ('AIR', 'AIR REG') " +
    //                 "            and ol_shipinstruct = 'DELIVER IN PERSON' " +
    //                 "        ) " +
    //                 "   or " +
    //                 "    ( " +
    //                 "                i_id = ol_i_id " +
    //                 "            and i_brand = 'Brand#34' " +
    //                 "            and i_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') " +
    //                 "            and ol_quantity >= 20 and ol_quantity <= 20 + 10 " +
    //                 "            and i_size between 1 and 15 " +
    //                 "            and ol_shipmode in ('AIR', 'AIR REG') " +
    //                 "            and ol_shipinstruct = 'DELIVER IN PERSON' " +
    //                 "        );";
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
