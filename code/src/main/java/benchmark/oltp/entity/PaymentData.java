package benchmark.oltp.entity;/*
 * Copyright (C) 2022, Zirui Hu, Rong Yu, Jinkai Xu, Yao Luo, Qingshaui Wang
 */

public class PaymentData extends CommonEntityData {
        /* terminal input data */
        public int w_id;
        public int d_id;
        public int c_id;
        public int c_d_id;
        public int c_w_id;
        public String c_last;
        public double h_amount;

        /* terminal output data */
        public String w_name;
        public String w_street_1;
        public String w_street_2;
        public String w_city;
        public String w_state;
        public String w_zip;
        public String d_name;
        public String d_street_1;
        public String d_street_2;
        public String d_city;
        public String d_state;
        public String d_zip;
        public String c_first;
        public String c_middle;
        public String c_street_1;
        public String c_street_2;
        public String c_city;
        public Integer c_state;
        public String c_zip;
        public String c_phone;
        public String c_since;
        public String c_credit;
        public double c_credit_lim;
        public double c_discount;
        public double c_balance;
        public String c_data;
        public String h_date;

    @Override
    public String toString() {
        return "PaymentData{" +
                "w_id=" + w_id +
                ", d_id=" + d_id +
                ", c_id=" + c_id +
                ", c_d_id=" + c_d_id +
                ", c_w_id=" + c_w_id +
                ", c_last='" + c_last + '\'' +
                ", h_amount=" + h_amount +
                ", w_name='" + w_name + '\'' +
                ", w_street_1='" + w_street_1 + '\'' +
                ", w_street_2='" + w_street_2 + '\'' +
                ", w_city='" + w_city + '\'' +
                ", w_state='" + w_state + '\'' +
                ", w_zip='" + w_zip + '\'' +
                ", d_name='" + d_name + '\'' +
                ", d_street_1='" + d_street_1 + '\'' +
                ", d_street_2='" + d_street_2 + '\'' +
                ", d_city='" + d_city + '\'' +
                ", d_state='" + d_state + '\'' +
                ", d_zip='" + d_zip + '\'' +
                ", c_first='" + c_first + '\'' +
                ", c_middle='" + c_middle + '\'' +
                ", c_street_1='" + c_street_1 + '\'' +
                ", c_street_2='" + c_street_2 + '\'' +
                ", c_city='" + c_city + '\'' +
                ", c_state=" + c_state +
                ", c_zip='" + c_zip + '\'' +
                ", c_phone='" + c_phone + '\'' +
                ", c_since='" + c_since + '\'' +
                ", c_credit='" + c_credit + '\'' +
                ", c_credit_lim=" + c_credit_lim +
                ", c_discount=" + c_discount +
                ", c_balance=" + c_balance +
                ", c_data='" + c_data + '\'' +
                ", h_date='" + h_date + '\'' +
                '}';
    }
}
