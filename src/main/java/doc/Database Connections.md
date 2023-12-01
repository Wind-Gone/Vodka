# Database Connections

- [Database Connections](#database-connections)
    - [OceanBase](#oceanbase)
        - [1. client连接](#1-client连接)
        - [2. 配置文件连接](#2-配置文件连接)
        - [3. 操作指令](#3-操作指令)
    - [Postgres Streaming Relication Mode](#postgres-streaming-relication-mode)
        - [1. client连接](#1-client连接-1)
        - [2. 配置文件连接](#2-配置文件连接-1)
        - [3. 操作指令](#3-操作指令-1)
    - [TiDB](#tidb)
        - [1. 启动33集群](#1-启动33集群)
        - [2. 连接方式](#2-连接方式)
        - [3. 配置文件连接](#3-配置文件连接)
    - [PolarDB](#polardb)
        - [1. 连接方式](#1-连接方式)

## OceanBase

### 1. client连接

```bash
mysql -uroot@mysql -h49.52.27.20 -P2883
mysql -uroot@mysql -h49.52.27.33 -P2881
```

### 2. 配置文件连接

```properties
db=oceanbase
driver=com.mysql.jdbc.Driver
conn=jdbc:mysql://49.52.27.20:2883/benchmarksql?rewriteBatchedStatements=true&allowMultiQueries=true&useLocalSessionState=true&useUnicode=true&characterEncoding=utf-8&socketTimeout=3000000
user=root@test
password=
```

### 3. 操作指令

#### 1. 创建数据库

```sql
create database benchmarksql default character set utf8 collate utf8_general_ci;
use benchmarksql;`
```

#### 2. 运行前手动执行一次Merge

```sql
mysql -uroot@sys -h49.52.27.20 -P2885  #切换到sys租户
use oceanbase;
ALTER SYSTEM MAJOR FREEZE TENANT = mysql; 		#执行merge
SELECT * FROM oceanbase.CDB_OB_ZONE_MAJOR_COMPACTION;	#下面两条指令都可用来判断是否merge完全，如果全部idle则compaction完成
select FROZEN_SCN, LAST_SCN FROM oceanbase.CDB_OB_MAJOR_COMPACTION; #如果两者相等，则compaction完成
```

#### 3. 重装系统的方式

```BASH
cd /home/xjk/oceanbase-all-in-one/conf/autodeploy
obd cluster autodeploy obtest -c default-example-bak.yaml (所有的更新全部选N)
cd /home/xjk && mysql --host 49.52.27.20 --port 2885 -Doceanbase -uroot < create_one_tenant_using_all_resources.pl

// 重装系统后需要重新设置的变量
alter system set enable_sql_extension=True;
set global ob_query_timeout = 36000000000;
set global ob_trx_timeout = 36000000000;
set global max_allowed_packet = 67108864;
set global parallel_servers_target = 2256;
```

#### 4.  备用指令

```sql
// tpch 调优指令
set global ob_sql_work_area_percentage = 80;
set global optimizer_use_sql_plan_baselines = True;
set global optimizer_capture_sql_plan_baselines = True;
set global _groupby_nopushdown_cut_ratio = 1;
set global secure_file_priv = '';

// 并行度
set _force_parallel_query_dop = 256;
[ERROR] Another app is currently holding the obd lock
ps aux | grep "obd" && sudo kill -9 [thread-id]
obd cluster edit-config obtest
```

## Postgres Streaming Relication Mode

### 1. client连接

```sql
psql -h49.52.27.33 -Upostgres -p5532
```

### 2. 配置文件连接

```properties
db=postgres
driver=org.postgresql.Driver
conn=jdbc:postgresql://49.52.27.33:5532/benchmarksql
user=postgres
password=
```

### 3. 操作指令

```sql
1. create database benchmarksql	# 创建数据库 
2. \dt show tables	# 显示表
3.select name, setting, min_val, max_val, context #查看buffer size
from pg_settings
where name = 'shared_buffers';
4. show max_parallel_workers_per_gather;
5. alter system set max_parallel_workers_per_gather=16;	# 并行度
6. SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='chbenchmark' AND pid<>pg_backend_pid();  # 强行关闭某个数据库的全部连接（适用于程序被手动终止，再次运行时发现程序卡死）
```

## TiDB

### 1. 启动33集群

```bash
su tidb-1 && cd ~ && source .bash_profile && tiup cluster start tidb-test
paswword: tidb
tiup cluster deploy tidb-test v7.1.0 tidb209.yaml
SET PASSWORD FOR 'root'@'%' = '123456';
```

### 2. 连接方式

```sql
mysql -uroot -h49.52.27.33 -P4002 -p123456
或者mysql -uroot -h49.52.27.33 -P3390 -p123456(应用了haproxy实现负载均衡)
```

### 3. 配置文件连接

```properties
db=tidb
driver=com.mysql.jdbc.Driver
conn=jdbc:mysql://49.52.27.33:3390/benchmarksql?rewriteBatchedStatements=true&allowMultiQueries=true&useLocalSessionState=true&useUnicode=true&characterEncoding=utf-8&socketTimeout=3000000
user=root
password=123456
```

### 4. 操作指令

```sql
1. set global @@tidb_max_tiflash_threads = 16; #tiflash并行度
```

## PolarDB

### 1. 连接方式

```bash
读写：psql -h 49.52.27.33 -p 5432 -U postgres -d t
只读1：psql -h 49.52.27.34 -p 5432 -U postgres -d t
只读2：psql -h 49.52.27.35 -p 5432 -U postgres -d t
```

### 2. 配置文件连接

```properties

```

### 3. 操作指令

```sql

```

## Linux指令

```bash
1. sudo sh -c "echo 3 > /proc/sys/vm/drop_caches" # 清理内存及cache
```

# 参考链接

1. https://www.oceanbase.com/docs/community-observer-cn-10000000000901975
2. https://docs.pingcap.com/zh/tidb/dev/dynamic-config#%E5%9C%A8%E7%BA%BF%E4%BF%AE%E6%94%B9-tiflash-%E9%85%8D%E7%BD%AE
3. https://www.oceanbase.com/docs/enterprise-oceanbase-database-cn-10000000000881094
4. https://help.aliyun.com/document_detail/215952.html?spm=a2c4g.215953.0.0.39936c8dE6lKNn
5. https://www.oceanbase.com/docs/community-observer-cn-10000000000013967
6. https://www.oceanbase.com/en/docs/community-observer-en-10000000000209672

