alter table vodka_warehouse
    add constraint vodka_warehouse_pkey
        primary key (w_id);

alter table vodka_district
    add constraint vodka_district_pkey
        primary key (d_w_id, d_id);

alter table vodka_customer
    add constraint vodka_customer_pkey
        primary key (c_w_id, c_d_id, c_id);

create index vodka_customer_idx1
    on vodka_customer (c_w_id, c_d_id, c_last, c_first);

alter table vodka_oorder
    add constraint vodka_oorder_pkey
        primary key (o_w_id, o_d_id, o_id);

create unique index vodka_oorder_idx1
    on vodka_oorder (o_w_id, o_d_id, o_carrier_id, o_id);

alter table vodka_new_order
    add constraint vodka_new_order_pkey
        primary key (no_w_id, no_d_id, no_o_id);

alter table vodka_order_line
    add constraint vodka_order_line_pkey
        primary key (ol_w_id, ol_d_id, ol_o_id, ol_number);

alter table vodka_stock
    add constraint vodka_stock_pkey
        primary key (s_w_id, s_i_id);

alter table vodka_item
    add constraint vodka_item_pkey
        primary key (i_id);

alter table vodka_nation
    add constraint nation_pkey primary key (n_nationkey);
alter table vodka_region
    add constraint region_pkey primary key (r_regionkey);
alter table vodka_supplier
    add constraint supplier_pkey primary key (s_suppkey);

create index vodka_order_line_ol_i_id_index on vodka_order_line (ol_i_id);
create index supplier_s_nationkey_index on vodka_supplier (s_nationkey);
create index vodka_customer_c_state_index on vodka_customer (c_nationkey);
create index nation_n_regionkey_index on vodka_nation (n_regionkey);
create index vodka_stock_s_tocksuppkey_index on vodka_stock (s_tocksuppkey);
create index vodka_stock_s_i_id_index on vodka_stock (s_i_id);
create index vodka_order_line_ol_suppkey_index on vodka_order_line (ol_suppkey);
create index vodka_order_line_ol_delivery_d_index on vodka_order_line (ol_delivery_d);
create index vodka_oorder_entry_d_index on vodka_oorder (o_entry_d);
create index vodka_history_h_date_index on vodka_history (h_date);
analyse;
