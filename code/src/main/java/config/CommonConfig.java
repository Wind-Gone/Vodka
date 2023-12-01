package config;/*
 * Copyright (C) 2022, Zirui Hu, Rong Yu, Jinkai Xu, Yao Luo, Qingshuai Wang
 */

import java.text.SimpleDateFormat;

public interface CommonConfig {
    String VdokaVersion = "1.0 DEV";

    int DB_UNKNOWN = 0,
            DB_FIREBIRD = 1,
            DB_ORACLE = 2,
            DB_POSTGRES = 3,
            DB_OCEANBASE = 4,
            DB_TIDB = 5,
            DB_POLARDB = 6,
            DB_GAUSSDB = 7,
            DB_MYSQL = 8;

    int NEW_ORDER = 1,
            PAYMENT = 2,
            ORDER_STATUS = 3,
            DELIVERY = 4,
            STOCK_LEVEL = 5,
            RECIEVE_GOODS = 6;

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


}
