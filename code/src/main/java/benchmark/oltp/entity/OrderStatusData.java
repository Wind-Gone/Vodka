package benchmark.oltp.entity;

import java.sql.Timestamp;
import java.util.Arrays;

public class OrderStatusData extends CommonEntityData {
    /* terminal input data */
    public int w_id;
    public int d_id;
    public int c_id;
    public String c_last;

    /* terminal output data */
    public String c_first;
    public String c_middle;
    public double c_balance;
    public int o_id;
    public Timestamp o_entry_d;
    public int o_carrier_id;

    public int[] ol_supply_w_id = new int[15];
    public int[] ol_i_id = new int[15];
    public int[] ol_quantity = new int[15];
    public double[] ol_amount = new double[15];
    public String[] ol_delivery_d = new String[15];

    @Override
    public String toString() {
        return "OrderStatusData{" +
                "w_id=" + w_id +
                ", d_id=" + d_id +
                ", c_id=" + c_id +
                ", c_last='" + c_last + '\'' +
                ", c_first='" + c_first + '\'' +
                ", c_middle='" + c_middle + '\'' +
                ", c_balance=" + c_balance +
                ", o_id=" + o_id +
                ", o_entry_d=" + o_entry_d +
                ", o_carrier_id=" + o_carrier_id +
                ", ol_supply_w_id=" + Arrays.toString(ol_supply_w_id) +
                ", ol_i_id=" + Arrays.toString(ol_i_id) +
                ", ol_quantity=" + Arrays.toString(ol_quantity) +
                ", ol_amount=" + Arrays.toString(ol_amount) +
                ", ol_delivery_d=" + Arrays.toString(ol_delivery_d) +
                '}';
    }
}
