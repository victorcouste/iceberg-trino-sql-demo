
USE iceberg_catalog.iceberg_schema;

DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS nation;
DROP SCHEMA IF EXISTS iceberg;

CREATE SCHEMA iceberg WITH (location = 's3://starburst/iceberg');

CREATE TABLE nation AS SELECT * FROM tpch.sf1.nation;

CREATE TABLE orders WITH (partitioning = ARRAY['year(orderdate)']) AS SELECT * FROM tpch.sf1.orders;
SELECT orderdate from orders WHERE orderdate>=date('1992-11-01') and orderdate<date('1993-01-01');

CREATE TABLE customer
WITH (
  partitioning = ARRAY['mktsegment'],
  format='parquet'
--location = 's3://my-bucket/a/path/'
) AS
SELECT
    c.custkey,
    c.name,
    c.mktsegment,
    ROUND(c.acctbal) as account_balance,
    n.name as nation,
    r.r_name as region
FROM
    postgresql.demo.customer c
    join oracle.demo.nation n on c.nationkey = n.nationkey
    join snowflake.demo.region r on r.r_regionkey = n.regionkey;

SELECT * FROM customer;

--------- Metadata Tables & Columns -----------------------------------------------------------------------------

SELECT custkey, "$path", "$file_modified_time" FROM customer;

SELECT * FROM "customer$properties";
SELECT * FROM "customer$snapshots";
SELECT * FROM "customer$history";
SELECT * FROM "customer$manifests";
SELECT * FROM "customer$partitions";
SELECT * FROM "customer$files";

------------- INSERT / UPDATE / DELETE / MERGE  ------------------------------------------------------------------

SELECT * FROM customer ORDER BY name;

INSERT INTO customer (custkey, name, mktsegment, account_balance, nation, region)
VALUES (200000, 'COMMANDER BUN BUN', 'SQLENGINE', 1, 'FRANCE', 'EUROPE');

SELECT * FROM customer ORDER BY name;
SELECT * FROM "customer$snapshots" ORDER BY committed_at DESC;

UPDATE customer SET account_balance = 1000 WHERE custkey = 200000;
SELECT * FROM customer ORDER BY name;
SELECT * FROM "customer$snapshots" ORDER BY committed_at DESC;

DELETE FROM customer WHERE custkey = 200000;
SELECT * FROM customer ORDER BY name;
SELECT * FROM "customer$snapshots" ORDER BY committed_at DESC;

-- MERGE INTO s3lakehouse.blog.customer_base AS b
-- USING s3lakehouse.blog.customer_land AS l
-- ON (b.custkey = l.custkey)
-- WHEN MATCHED and b.name != l.name
-- THEN UPDATE
-- SET name = l.name ,
--     state = l.state,
--     zip = l.zip,
--     cust_since = l.cust_since
-- WHEN NOT MATCHED
--       THEN INSERT (custkey, name, state, zip, cust_since,last_update_dt)
--             VALUES(l.custkey, l.name, l.state, l.zip, l.cust_since,l.last_update_dt);

------- ALTER / Schema Evolution --------------------------------------------------------------------------------

ALTER TABLE customer ADD COLUMN phone varchar;
INSERT INTO customer (custkey, name, mktsegment, account_balance, nation, region, phone)
VALUES (200000, 'COMMANDER BUN BUN', 'SQLENGINE', 1, 'FRANCE', 'EUROPE','+33606060606');
SELECT * FROM customer ORDER BY name;
SELECT * FROM "customer$snapshots" ORDER BY committed_at DESC;

------- Partition Evolution ---------------------------------------------------------------------------------------

ALTER TABLE customer SET PROPERTIES partitioning = ARRAY['mktsegment', 'custkey'];

INSERT INTO customer (custkey, name, mktsegment, account_balance, nation, region, phone) VALUES (200001, 'TRINO', 'SQLENGINE', 1, 'FRANCE', 'EUROPE','+33606060606');
INSERT INTO customer (custkey, name, mktsegment, account_balance, nation, region, phone) VALUES (200002, 'STARBURST', 'SQLENGINE', 2, 'FRANCE', 'EUROPE','+33606060606');

SELECT * FROM "customer$snapshots" ORDER BY committed_at DESC;
SELECT * FROM "customer$partitions";

ALTER TABLE customer SET PROPERTIES partitioning = ARRAY['nation'];

INSERT INTO customer (custkey, name, mktsegment, account_balance, nation, region, phone) VALUES (200001, 'TRINO', 'SQLENGINE', 1, 'ITALY', 'EUROPE','+33606060606');
INSERT INTO customer (custkey, name, mktsegment, account_balance, nation, region, phone) VALUES (200002, 'STARBURST', 'SQLENGINE', 2, 'SPAIN', 'EUROPE','+33606060606');

SELECT * FROM "customer$snapshots" ORDER BY committed_at DESC;
SELECT * FROM "customer$partitions";

------- Time Travel / Snapshots -----------------------------------------------------------------------------------------

SELECT * FROM "customer$snapshots" ORDER BY committed_at DESC;

SELECT * FROM customer;
SELECT * FROM customer FOR VERSION AS OF 1034832818467019325  ORDER BY name;

CALL system.rollback_to_snapshot('iceberg', 'customer', 1034832818467019325);
SELECT * FROM customer;

------- Optimize / Compaction - Vacuum / Cleaning snapshots and orphan files --------------------------

ALTER TABLE customer EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE customer EXECUTE remove_orphan_files(retention_threshold => '7d');
ALTER TABLE customer EXECUTE optimize(file_size_threshold => '10MB');

-- ALTER TABLE customer EXECUTE optimize WHERE $file_modified_time > <yesterday>

-- ALTER TABLE customer
--   EXECUTE OPTIMIZE
--   WHERE CAST(timestamp AS DATE) >= CAST(now() - INTERVAL '2' DAY AS DATE);

------- Get Columns Statistics for Trino CBO -----------------------------------------------------------

SET SESSION experimental_extended_statistics_enabled=true;
ANALYZE customer;
SHOW STATS FOR customer;

------- Federation with Iceberg data --------------------------------------------------------------------------------

SELECT
    c.nation as nation,
    round(sum(o.totalprice)) as total_price
FROM
    customer c
    join postgresql.demo.orders o on c.custkey = o.custkey
WHERE 
    c.region='EUROPE'
GROUP BY c.nation
ORDER BY total_price;