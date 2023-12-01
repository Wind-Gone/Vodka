package benchmark.oltp.entity;/*
 * Copyright (C) 2022, Zirui Hu, Rong Yu, Jinkai Xu, Yao Luo, Qingshaui Wang
 */

public class DeliveryData extends CommonEntityData {
    /* terminal input data */
    public int w_id;
    public int o_carrier_id;

    /* terminal output data */
    public String execution_status;

    /*
     * executeDelivery() will store the background request
     * here for the caller to pick up and process as needed.
     */
    public OLTPData deliveryBG;

    @Override
    public String toString() {
        return "DeliveryData{" +
                "w_id=" + w_id +
                ", o_carrier_id=" + o_carrier_id +
                ", execution_status='" + execution_status + '\'' +
                ", deliveryBG=" + deliveryBG +
                '}';
    }
}
