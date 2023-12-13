alter table vodka_district
    add constraint d_warehouse_fkey
        foreign key (d_w_id)
            references vodka_warehouse (w_id);

alter table vodka_customer
    add constraint c_district_fkey
        foreign key (c_w_id, c_d_id)
            references vodka_district (d_w_id, d_id);

alter table vodka_history
    add constraint h_customer_fkey
        foreign key (h_c_w_id, h_c_d_id, h_c_id)
            references vodka_customer (c_w_id, c_d_id, c_id);
alter table vodka_history
    add constraint h_district_fkey
        foreign key (h_w_id, h_d_id)
            references vodka_district (d_w_id, d_id);

alter table vodka_new_order
    add constraint no_order_fkey
        foreign key (no_w_id, no_d_id, no_o_id)
            references vodka_oorder (o_w_id, o_d_id, o_id);

alter table vodka_oorder
    add constraint o_customer_fkey
        foreign key (o_w_id, o_d_id, o_c_id)
            references vodka_customer (c_w_id, c_d_id, c_id);

alter table vodka_order_line
    add constraint ol_order_fkey
        foreign key (ol_w_id, ol_d_id, ol_o_id)
            references vodka_oorder (o_w_id, o_d_id, o_id);

alter table vodka_order_line
    add constraint ol_stock_fkey
        foreign key (ol_supply_w_id, ol_i_id)
            references vodka_stock (s_w_id, s_i_id);

alter table vodka_stock
    add constraint s_warehouse_fkey
        foreign key (s_w_id)
            references vodka_warehouse (w_id);

alter table vodka_stock
    add constraint s_item_fkey
        foreign key (s_i_id)
            references vodka_item (i_id);
            
alter table vodka_nation
    add constraint n_region_fkey
        foreign key (n_regionkey)
            references vodka_region (r_regionkey);

alter table vodka_supplier
    add constraint su_nation_fkey
        foreign key (s_nationkey)
            references vodka_nation (n_nationkey);

alter table vodka_stock
    add constraint s_supplier_fkey
        foreign key (s_tocksuppkey)
            references vodka_supplier (s_suppkey);

alter table vodka_customer
    add constraint c_nationkey_fkey
        foreign key (c_nationkey)
            references vodka_nation (n_nationkey)
