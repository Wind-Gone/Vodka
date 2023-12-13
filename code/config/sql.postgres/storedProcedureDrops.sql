drop function if exists vodka_proc_new_order (integer, integer, integer, integer[], integer[], integer[]);
drop function if exists vodka_proc_stock_level(integer, integer, integer);
drop function if exists vodka_proc_payment(integer, integer, integer, integer, integer, varchar(16), decimal(6,2));
drop function if exists vodka_proc_order_status (integer, integer, integer, var(16));
drop function if exists vodka_cid_from_clast(integer, integer, varchar(16));
drop function if exists vodka_proc_delivery_bg (integer, integer, integer);
