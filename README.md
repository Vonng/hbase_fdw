# HBase FDW

### Introduction
HBase FDW is a [PostgreSQL](https://www.postgresql.org/) FDW ([foregin data wrapper](https://wiki.postgresql.org/wiki/Foreign_data_wrappers) ) based on [Multicorn](http://multicorn.org/) in order to manipulate [HBase](http://hbase.apache.org/) from PostgreSQL.

## Structure
* hbase_fdw:    Source code folder, Should put it into your python library path
  * [happybase_fdw.py](hbase_fdw/happybase_fdw.py)  hbase_fdw based on happy base 
* resource:     sql files
  * [happybase_fdw_test.sql](resource/happybase_fdw_test.sql) Setup and test script for happybase_fdw

## Shortcut
Before execute `make`, change `$TARGET_DIR` to your python lib path.

e.g :`/usr/local/anaconda/lib/python2.7/site-packages`

```bash
	sudo make link		soft link hbase_fdw into your $TARGET_DIR. Convient for debugging.
	sudo make install 	copy hbase_fdw to your $TARGET_DIR.
	sudo make uninstall delete hbase_fdw from your $TARGET_DIR.
	sudo make setup		quick test (well at least you need a hbase thrift service first....)
```

## Usage:

#### Setup:
```sql
CREATE DATABASE fdw;

CREATE EXTENSION multicorn;

CREATE SERVER hserver FOREIGN DATA WRAPPER multicorn OPTIONS ( WRAPPER 'hbase_fdw.HappyBaseFdw');
```


#### DDL:
This is the form I normally use.
```sql
CREATE FOREIGN TABLE IF NOT EXISTS hbtest (
  rowkey    TEXT,
  timestamp TIMESTAMP,
  active    INTEGER OPTIONS (qualifier '1_day_active_count'),
  install   INTEGER OPTIONS (qualifier '1_day_install_count'),
  launch    INTEGER OPTIONS (qualifier '1_day_launch_count')
) SERVER hserver
OPTIONS (host '10.101.171.99',port '9090', table 'appuserstat', debug 'False', family 'stat');
```

There are some constraint about HBase foreign table DDL:
* `rowkey` and `timestamp` is special in DDL.
* `rowkey` is required, and it's type must be one of `BYTEA` or `TEXT`.
* `timestamp` is optional. and it could be typed as BIGINT(raw hbase timestamp), `DATE`, `TIMESTAMP`, `TIMESTAMPTZ`.
* other columns will be normal hbase columns. And cf:qual will be formed according to following rules:
    * if column option `qualifier` and table option `family` is specified: `family_option+qualifier_option`
    * if only column option `qualifier` is specified: `qualifier_option`
    * if only table option `family` is specified: `family_option + column_name`
    * if nether column option `qualifier` and table option `family` is specified: `col_name.replace('_', ':', 1)`

You could set all normal columns type to `BYTEA`. and use `convert_from` to translate it into wanted type:
```sql
CREATE FOREIGN TABLE IF NOT EXISTS hbtest (
  rowkey    BYTEA,
  timestamp BIGINT,
  stat_1_day_active_count  BYTEA,
  stat_1_day_install_count BYTEA,
  stat_1_day_launch_count  BYTEA
) SERVER hserver
OPTIONS (host '10.101.171.99',port '9090', table 'appuserstat');
```
This is more like hbase's data model: really rough. And corresponding SQL would be like:

```sql
SELECT
  convert_from(rowkey,'UTF8') as rowkey,
  timestamp,
  convert_from(stat_1_day_active_count,'UTF8')::INTEGER as active,
  convert_from(stat_1_day_install_count,'UTF8')::INTEGER as install,
  convert_from(stat_1_day_launch_count,'UTF8')::INTEGER as launch
FROM hbtest2
WHERE rowkey = '9c9e_2016-02-02_56444370e7e12af0561e221c' ;
```



#### Query:

##### Single selection
```sql
SELECT * FROM hbtest
WHERE rowkey = '9c9e_2016-02-02_56444370e7e12af0561e221c' and timestamp < '2016-03-01';
```

```bash
                  rowkey                  |      timestamp      | active | install | launch
------------------------------------------+---------------------+--------+---------+--------
 9c9e_2016-02-02_56444370e7e12af0561e221c | 2016-02-03 00:00:00 |   9945 |    1042 |  42748
```

##### Multiple selection
```sql
SELECT * FROM hbtest WHERE hbtest.timestamp < now() and rowkey IN (
  '9c9e_2016-02-02_56444370e7e12af0561e221c',
  'd58c_2015-12-03_548935a4fd98c5d3510008bc',
  'b50d_2015-12-03_5506905ffd98c5ae1b0000de',
  'e18d_2015-12-03_559e9b1067e58e2cdd002509',
  '8545_2015-12-03_563b1f8f67e58e55580014d1',
  '1516_2015-12-03_56430770cc3e5975ca000012');
```
```
                  rowkey                  |      timestamp      | active | install | launch
------------------------------------------+---------------------+--------+---------+---------
 9c9e_2016-02-02_56444370e7e12af0561e221c | 2016-02-03 00:00:00 |   9945 |    1042 |   42748
 d58c_2015-12-03_548935a4fd98c5d3510008bc | 2015-12-07 00:00:00 |      0 |       0 |       0
 b50d_2015-12-03_5506905ffd98c5ae1b0000de | 2015-12-07 00:00:00 | 516758 |   92292 | 2123118
 e18d_2015-12-03_559e9b1067e58e2cdd002509 | 2015-12-07 00:00:00 |  87583 |    7075 | 1068343
 8545_2015-12-03_563b1f8f67e58e55580014d1 | 2015-12-07 00:00:00 |      0 |       0 |       0
 1516_2015-12-03_56430770cc3e5975ca000012 | 2015-12-07 00:00:00 |      0 |       0 |       0
```

##### Range Scan
```sql
SELECT rowkey,active,install,launch FROM hbtest
WHERE rowkey BETWEEN '9c9a' AND '9c9c' AND active > 0 and install > 0 and rowkey ~ '^.{4}_.{10}_\w{24}' LIMIT 10;
```
```
                                 rowkey                                 | active | install | launch
------------------------------------------------------------------------+--------+---------+--------
 9c9a_2015-07-29_4f28e09752701567d0000087&&androidmarket\x1F##1.9.1     |     47 |      15 |    279
 9c9a_2015-08-01_50b57d2152701505140000aa&&A-goapk\x1F##4.1.9           |    146 |       1 |     71
 9c9a_2015-08-16_50b57d2152701505140000aa&&moxiu-date\x1F##4.5.9        |     12 |       1 |      7
 9c9a_2015-09-17_50b57d2152701505140000aa&&new-moxiulauncher\x1F##4.9.1 |   1675 |      11 |    727
 9c9a_2015-09-28_50b57d2152701505140000aa&&B-jinli\x1F##5.0.4           |    721 |       3 |    359
 9c9a_2015-12-29_4e2f707f431fe371c4000242&&setup\x1F##2.9.0             |     22 |       1 |     67
 9c9a_2016-01-05_56444368e7e12af0561e2215&&mtxx_aux_common              |    143 |      10 |    393
 9c9a_2016-01-08_5644436fe7e12af0561e221b&&z-dianchiyisheng\x1F##5.3.2  |    472 |       1 |  25315
 9c9a_2016-01-30_5644436fe7e12af0561e221b&&A-wandoujia\x1F##5.2.6       |   6864 |       3 | 365874
 9c9a_2016-03-15_56444368e7e12af0561e2215&&coolpad\x1F##4.2.0           |     78 |       2 |    187
```



##### Test CRUD

```sql
SELECT rowkey,active,install,launch FROM hbtest where rowkey in ('hbtest1','hbtest2','hbtest3');
```
```
 rowkey | active | install | launch
--------+--------+---------+--------
```




```sql
INSERT INTO hbtest (rowkey, active, install, launch) VALUES ('hbtest1', 1, 2, 3),('hbtest2', 1, 2, 3);
```
```
INSERT 0 2
```




```sql
SELECT rowkey,active,install,launch FROM hbtest where rowkey in ('hbtest1','hbtest2','hbtest3');
```
```
 rowkey  | active | install | launch
---------+--------+---------+--------
 hbtest1 |      1 |       2 |      3
 hbtest2 |      1 |       2 |      3
(2 rows)
```




```sql
DELETE FROM hbtest where rowkey = 'hbtest1';
```
```
DELETE 1
```




```sql
UPDATE hbtest set active = 999 where rowkey = 'hbtest2';
```
```
UPDATE 1
```



```sql
SELECT rowkey,active,install,launch FROM hbtest where rowkey in ('hbtest1','hbtest2', 'hbtest3');
```
 ```
 rowkey  | active | install | launch
---------+--------+---------+--------
 hbtest2 |    999 |       2 |      3
(1 row)
 ```



```sql
DELETE FROM hbtest where rowkey in ('hbtest1','hbtest2', 'hbtest3');
```
```
DELETE 1
```



### Note:

I use it in a small monitor system. and a low qps query situation.

As you can see. Happybase and thrift server may not be a proper way to support a production system...

