-- =================================================== --
-- Ensure Multicorn is installed
CREATE EXTENSION IF NOT EXISTS multicorn;

-- HBase Server
CREATE SERVER hserver FOREIGN DATA WRAPPER multicorn
OPTIONS ( WRAPPER 'hbase_fdw.HappyBaseFdw');

-- Create HBase Test Table
CREATE FOREIGN TABLE hbtest (
  rowkey    TEXT OPTIONS (algo 'prefix4-md5'),
  timestamp INTEGER,
  app_id    TEXT,
  date      DATE,
  active    BIGINT,
  install   BIGINT,
  launch    BIGINT,
  pay       BIGINT
) SERVER hserver OPTIONS (MODE 'dev'
);


SELECT *
FROM hbtest;