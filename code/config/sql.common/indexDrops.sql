alter table vodka_warehouse drop constraint vodka_warehouse_pkey;

alter table vodka_district drop constraint vodka_district_pkey;

alter table vodka_customer drop constraint vodka_customer_pkey;
drop index vodka_customer_idx1;

alter table vodka_oorder drop constraint vodka_oorder_pkey;
drop index vodka_oorder_idx1;

alter table vodka_new_order drop constraint vodka_new_order_pkey;

alter table vodka_order_line drop constraint vodka_order_line_pkey;

alter table vodka_stock drop constraint vodka_stock_pkey;

alter table vodka_item drop constraint vodka_item_pkey;

drop index vodka_order_line_ol_i_id_index;
drop index supplier_s_nationkey_index;
drop index vodka_customer_c_state_index;
drop index nation_n_regionkey_index;
drop index vodka_stock_s_tocksuppkey_index;
drop index vodka_stock_s_i_id_index;
drop index vodka_order_line_ol_suppkey_index;
drop index vodka_order_line_ol_delivery_d_index;
drop index vodka_oorder_entry_d_index;