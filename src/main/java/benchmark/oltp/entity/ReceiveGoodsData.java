package benchmark.oltp.entity;/*
 * Copyright (C) 2022, Zirui Hu, Rong Yu, Jinkai Xu, Yao Luo, Qingshaui Wang
 */

import java.sql.Timestamp;

public class ReceiveGoodsData extends CommonEntityData {
    /* RECEIVEGOODS data */
    public int w_id;
    public int d_id;
    public int o_id;
    public long ol_receipdate;
    public Timestamp ol_delivery_d;
    public String ol_returnflag;
    public String execution_status;


    @Override
    public String toString() {
        return "RecieveGoodsData{" +
                "w_id=" + w_id +
                ", d_id=" + d_id +
                ", o_id=" + o_id +
                ", ol_receipdate=" + ol_receipdate +
                ", ol_delivery_d=" + ol_delivery_d +
                ", ol_returnflag='" + ol_returnflag + '\'' +
                ", execution_status='" + execution_status + '\'' +
                '}';
    }
}
