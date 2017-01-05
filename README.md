# HBase FDW

### Introduction
HBase FDW is a postgres foregin data wrapper based on multicorn in order to manipulate hbase from postgres.

## Structure
* hbase_fdw:    Source code folder, Should put into your python library path
* resource:     sql files: DDL, Test , etc...

### Shortcut
```
	sudo make link		链接当前目录至Python Path,便于调试
	sudo make install 	拷贝本程序至Python Path
	sudo make uninstall 卸载本程序
	sudo make setup		设置happybase_fdw并进行功能测试
```

### Usage:
```
-- psql elysium < resource/happybase_fdw_test.sql --echo-all

-- =================================================== --
-- Ensure Multicorn is installed
-- CREATE EXTENSION IF NOT EXISTS multicorn;
-- HBase Server
DROP SERVER IF EXISTS hserver CASCADE;
NOTICE:  drop cascades to foreign table hbtest
DROP SERVER
CREATE SERVER hserver FOREIGN DATA WRAPPER multicorn
OPTIONS ( WRAPPER 'hbase_fdw.HappyBaseFdw');
CREATE SERVER
-- Create HBase Test Table
DROP FOREIGN TABLE IF EXISTS hbtest;
NOTICE:  foreign table "hbtest" does not exist, skipping
DROP FOREIGN TABLE
CREATE FOREIGN TABLE IF NOT EXISTS hbtest (
  rowkey  TEXT,
  timestamp TIMESTAMP,
  active  INTEGER OPTIONS (qualifier '1_day_active_count'),
  install INTEGER OPTIONS (qualifier '1_day_install_count'),
  launch  INTEGER OPTIONS (qualifier '1_day_launch_count')
) SERVER hserver OPTIONS (HOST '10.101.171.99',PORT '9090', TABLE 'appuserstat', DEBUG 'False', prefix 'stat'
);
CREATE FOREIGN TABLE
-- Single selection
SELECT * FROM hbtest
WHERE rowkey = '9c9e_2016-02-02_56444370e7e12af0561e221c' and timestamp < '2016-03-01';
                  rowkey                  |      timestamp      | active | install | launch
------------------------------------------+---------------------+--------+---------+--------
 9c9e_2016-02-02_56444370e7e12af0561e221c | 2016-02-03 00:00:00 |   9945 |    1042 |  42748
(1 row)

-- Multiple selection
SELECT * FROM hbtest WHERE hbtest.timestamp < now() and rowkey IN (
  '9c9e_2016-02-02_56444370e7e12af0561e221c',
  'd58c_2015-12-03_548935a4fd98c5d3510008bc',
  'b50d_2015-12-03_5506905ffd98c5ae1b0000de',
  'e18d_2015-12-03_559e9b1067e58e2cdd002509',
  '8545_2015-12-03_563b1f8f67e58e55580014d1',
  '1516_2015-12-03_56430770cc3e5975ca000012');
                  rowkey                  |      timestamp      | active | install | launch
------------------------------------------+---------------------+--------+---------+---------
 9c9e_2016-02-02_56444370e7e12af0561e221c | 2016-02-03 00:00:00 |   9945 |    1042 |   42748
 d58c_2015-12-03_548935a4fd98c5d3510008bc | 2015-12-07 00:00:00 |      0 |       0 |       0
 b50d_2015-12-03_5506905ffd98c5ae1b0000de | 2015-12-07 00:00:00 | 516758 |   92292 | 2123118
 e18d_2015-12-03_559e9b1067e58e2cdd002509 | 2015-12-07 00:00:00 |  87583 |    7075 | 1068343
 8545_2015-12-03_563b1f8f67e58e55580014d1 | 2015-12-07 00:00:00 |      0 |       0 |       0
 1516_2015-12-03_56430770cc3e5975ca000012 | 2015-12-07 00:00:00 |      0 |       0 |       0
(6 rows)

-- Range Scan
SELECT rowkey,active,install,launch FROM hbtest
WHERE rowkey BETWEEN '9c9a' AND '9c9c' AND active > 0 and install > 0 and rowkey ~ '^.{4}_.{10}_\w{24}' LIMIT 10;
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
(10 rows)

-- Test CRUD
SELECT rowkey,active,install,launch FROM hbtest where rowkey in ('hbtest1','hbtest2','hbtest3');
 rowkey | active | install | launch
--------+--------+---------+--------
(0 rows)

INSERT INTO hbtest (rowkey, active, install, launch) VALUES ('hbtest1', 1, 2, 3),('hbtest2', 1, 2, 3);
INSERT 0 2
SELECT rowkey,active,install,launch FROM hbtest where rowkey in ('hbtest1','hbtest2','hbtest3');
 rowkey  | active | install | launch
---------+--------+---------+--------
 hbtest1 |      1 |       2 |      3
 hbtest2 |      1 |       2 |      3
(2 rows)

DELETE FROM hbtest where rowkey = 'hbtest1';
DELETE 1
UPDATE hbtest set active = 999 where rowkey = 'hbtest2';
UPDATE 1
SELECT rowkey,active,install,launch FROM hbtest where rowkey in ('hbtest1','hbtest2', 'hbtest3');
 rowkey  | active | install | launch
---------+--------+---------+--------
 hbtest2 |    999 |       2 |      3
(1 row)

DELETE FROM hbtest where rowkey in ('hbtest1','hbtest2', 'hbtest3');
DELETE 1
```
