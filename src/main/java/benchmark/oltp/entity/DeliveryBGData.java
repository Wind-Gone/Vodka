package benchmark.oltp.entity;/*
 * Copyright (C) 2022, Zirui Hu, Rong Yu, Jinkai Xu, Yao Luo, Qingshaui Wang
 */

import lombok.ToString;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;

@ToString
public class DeliveryBGData extends CommonEntityData {
    /* DELIVERY_BG data */
    public int w_id;
    public int o_carrier_id;
    public Timestamp ol_delivery_d;

    public int[] delivered_o_id;
    public int[] delivered_c_id;
    public Date[] delivery_entry_date;
    public double[] sum_ol_amount;
}
