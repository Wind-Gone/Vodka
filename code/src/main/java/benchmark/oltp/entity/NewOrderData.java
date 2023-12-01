package benchmark.oltp.entity;

import java.sql.Timestamp;
import java.util.Arrays;

public class NewOrderData extends CommonEntityData {
    /* terminal input data */
    public int w_id;
    public int d_id;
    public int c_id;
    public int[] ol_supply_w_id = new int[15];
    public int[] ol_i_id = new int[15];
    public int[] ol_quantity = new int[15];
    /* terminal output data */
    public String c_last;
    public String c_credit;
    public double c_discount;
    public double w_tax;
    public double d_tax;
    public int o_ol_cnt;
    public int o_id;
    public Timestamp o_entry_d;
    public double total_amount;
    public String execution_status;
    public String[] i_name = new String[15];
    public int[] s_quantity = new int[15];
    public String[] brand_generic = new String[15];
    public double[] i_price = new double[15];
    public double[] ol_amount = new double[15];
    public String[] dist_value = new String[15];
    public boolean[] found = new boolean[15];

    @Override
    public String toString() {
        return "NewOrderData{" +
                "w_id=" + w_id +
                ", d_id=" + d_id +
                ", c_id=" + c_id +
                ", ol_supply_w_id=" + Arrays.toString(ol_supply_w_id) +
                ", ol_i_id=" + Arrays.toString(ol_i_id) +
                ", ol_quantity=" + Arrays.toString(ol_quantity) +
                ", c_last='" + c_last + '\'' +
                ", c_credit='" + c_credit + '\'' +
                ", c_discount=" + c_discount +
                ", w_tax=" + w_tax +
                ", d_tax=" + d_tax +
                ", o_ol_cnt=" + o_ol_cnt +
                ", o_id=" + o_id +
                ", o_entry_d=" + o_entry_d +
                ", total_amount=" + total_amount +
                ", execution_status='" + execution_status + '\'' +
                ", i_name=" + Arrays.toString(i_name) +
                ", s_quantity=" + Arrays.toString(s_quantity) +
                ", brand_generic=" + Arrays.toString(brand_generic) +
                ", i_price=" + Arrays.toString(i_price) +
                ", ol_amount=" + Arrays.toString(ol_amount) +
                '}';
    }
}

