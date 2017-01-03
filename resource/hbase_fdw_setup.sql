-- =================================================== --
-- Ensure Multicorn is installed
-- CREATE EXTENSION IF NOT EXISTS multicorn;

-- HBase Server
DROP SERVER IF EXISTS hserver CASCADE;
CREATE SERVER hserver FOREIGN DATA WRAPPER multicorn
OPTIONS ( WRAPPER 'hbase_fdw.HappyBaseFdw');

-- Create HBase Test Table
DROP FOREIGN TABLE IF EXISTS hbtest;
CREATE FOREIGN TABLE IF NOT EXISTS hbtest (
  rowkey  TEXT,
  active  INTEGER OPTIONS (qualifier '1_day_active_count'),
  install INTEGER OPTIONS (qualifier '1_day_install_count'),
  launch  INTEGER OPTIONS (qualifier '1_day_launch_count')
) SERVER hserver OPTIONS (host '10.101.171.99', TABLE 'appuserstat', DEBUG 'False', prefix 'stat'
);

-- Single selection
SELECT *
FROM hbtest
WHERE rowkey = '9c9e_2016-02-02_56444370e7e12af0561e221c';

-- Multiple selection
SELECT * FROM hbtest WHERE rowkey IN (
  '9c9e_2016-02-02_56444370e7e12af0561e221c',
  'd58c_2015-12-03_548935a4fd98c5d3510008bc',
  'b50d_2015-12-03_5506905ffd98c5ae1b0000de',
  'e18d_2015-12-03_559e9b1067e58e2cdd002509',
  '8545_2015-12-03_563b1f8f67e58e55580014d1',
  '1516_2015-12-03_56430770cc3e5975ca000012');

-- Range Scan
SELECT rowkey,active,install,launch FROM hbtest
WHERE rowkey BETWEEN '9c9a' AND '9c9c' AND active > 0 and install > 0 and rowkey ~ '^.{4}_.{10}_\w{24}' LIMIT 20;



SELECT * FROM hbtest where rowkey in ('hbtest1','hbtest2');

INSERT INTO hbtest (rowkey, active, install, launch) VALUES ('hbtest1', 1, 2, 3);
INSERT INTO hbtest (rowkey, active, install, launch) VALUES ('hbtest2', 1, 2, 3);

SELECT * FROM hbtest where rowkey in ('hbtest1','hbtest2');

DELETE FROM hbtest where rowkey = 'hbtest1';

SELECT * FROM hbtest where rowkey in ('hbtest1','hbtest2');
