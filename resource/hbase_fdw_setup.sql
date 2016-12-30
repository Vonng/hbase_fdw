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
  active  INTEGER OPTIONS (qualifier 'stat:1_day_active_count'),
  install BIGINT OPTIONS (qualifier 'stat:1_day_install_count'),
  launch  SMALLINT OPTIONS (qualifier 'stat:stat:1_day_launch_count')
) SERVER hserver OPTIONS (host '10.101.171.99', TABLE 'appuserstat', DEBUG 'True'
);

SELECT *
FROM hbtest
WHERE rowkey = '9c9e_2016-02-02_56444370e7e12af0561e221c';

SELECT *
FROM hbtest
WHERE rowkey IN ('9c9e_2016-02-02_56444370e7e12af0561e221c',
                 '9c9b_2016-02-02_56444370e7e12af0561e221c');

SELECT *
FROM hbtest
WHERE rowkey
BETWEEN '9c9a_2016-02-02_56444370e7e12af0561e221c' AND
'9c9f_2016-02-02_56444370e7e12af0561e221c';

--       AND app_id = '56444370e7e12af0561e221c'
--       AND date IN ('2016-02-02', '2016-02-03');

-- 9c9e_2016-02-02_56444370e7e12af0561e221c