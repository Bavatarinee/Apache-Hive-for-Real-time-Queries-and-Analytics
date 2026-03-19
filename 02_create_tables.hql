-- ============================================================
-- Script: 02_create_tables.hql
-- Purpose: Create staging (CSV) and optimized (ORC) tables
-- ============================================================

USE hive_analytics;

-- --------------------------------------------------------
-- STAGING TABLES (external CSV, for raw ingestion)
-- --------------------------------------------------------

-- Customers Staging
DROP TABLE IF EXISTS stg_customers;
CREATE EXTERNAL TABLE stg_customers (
    customer_id    STRING,
    name           STRING,
    email          STRING,
    age            INT,
    gender         STRING,
    region         STRING,
    city           STRING,
    segment        STRING,
    loyalty_score  DOUBLE,
    join_date      STRING
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/hive_analytics/stg_customers'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Products Staging
DROP TABLE IF EXISTS stg_products;
CREATE EXTERNAL TABLE stg_products (
    product_id   STRING,
    name         STRING,
    category     STRING,
    sub_category STRING,
    brand        STRING,
    base_price   DOUBLE,
    cost_price   DOUBLE,
    weight_kg    DOUBLE,
    warehouse_id STRING,
    stock_qty    INT,
    rating       DOUBLE
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/hive_analytics/stg_products'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Transactions Staging
DROP TABLE IF EXISTS stg_transactions;
CREATE EXTERNAL TABLE stg_transactions (
    transaction_id STRING,
    customer_id    STRING,
    product_id     STRING,
    category       STRING,
    region         STRING,
    city           STRING,
    quantity       INT,
    unit_price     DOUBLE,
    discount       DOUBLE,
    total_amount   DOUBLE,
    payment_method STRING,
    device_type    STRING,
    status         STRING,
    warehouse_id   STRING,
    ts             STRING,
    yr             INT,
    mn             INT,
    dy             INT,
    hr             INT
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/hive_analytics/stg_transactions'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Sensor Events Staging
DROP TABLE IF EXISTS stg_sensor_events;
CREATE EXTERNAL TABLE stg_sensor_events (
    event_id     STRING,
    sensor_id    STRING,
    sensor_type  STRING,
    warehouse_id STRING,
    value        DOUBLE,
    unit         STRING,
    is_alert     BOOLEAN,
    ts           STRING,
    yr           INT,
    mn           INT,
    dy           INT,
    hr           INT
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/hive_analytics/stg_sensor_events'
TBLPROPERTIES ('skip.header.line.count'='1');


-- --------------------------------------------------------
-- OPTIMIZED TABLES (ORC format, Partitioned + Bucketed)
-- --------------------------------------------------------
SET hive.exec.dynamic.partition        = true;
SET hive.exec.dynamic.partition.mode   = nonstrict;
SET hive.enforce.bucketing             = true;
SET hive.exec.max.dynamic.partitions   = 10000;

-- Customers (ORC, no partition needed -- dimension table)
DROP TABLE IF EXISTS dim_customers;
CREATE TABLE dim_customers (
    customer_id    STRING,
    name           STRING,
    email          STRING,
    age            INT,
    gender         STRING,
    city           STRING,
    segment        STRING,
    loyalty_score  DOUBLE,
    join_date      DATE
)
PARTITIONED BY (region STRING)
CLUSTERED BY (customer_id) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true', 'orc.compress'='SNAPPY');

-- Products (ORC)
DROP TABLE IF EXISTS dim_products;
CREATE TABLE dim_products (
    product_id   STRING,
    name         STRING,
    sub_category STRING,
    brand        STRING,
    base_price   DOUBLE,
    cost_price   DOUBLE,
    weight_kg    DOUBLE,
    warehouse_id STRING,
    stock_qty    INT,
    rating       DOUBLE
)
PARTITIONED BY (category STRING)
CLUSTERED BY (product_id) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true', 'orc.compress'='SNAPPY');

-- Transactions Fact Table (ORC, partitioned by year/month)
DROP TABLE IF EXISTS fact_transactions;
CREATE TABLE fact_transactions (
    transaction_id STRING,
    customer_id    STRING,
    product_id     STRING,
    city           STRING,
    quantity       INT,
    unit_price     DOUBLE,
    discount       DOUBLE,
    total_amount   DOUBLE,
    payment_method STRING,
    device_type    STRING,
    status         STRING,
    warehouse_id   STRING,
    ts             TIMESTAMP,
    dy             INT,
    hr             INT
)
PARTITIONED BY (region STRING, category STRING, yr INT, mn INT)
CLUSTERED BY (customer_id) INTO 16 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true', 'orc.compress'='SNAPPY');

-- Sensor Events (ORC, partitioned by warehouse and year/month)
DROP TABLE IF EXISTS fact_sensor_events;
CREATE TABLE fact_sensor_events (
    event_id     STRING,
    sensor_id    STRING,
    sensor_type  STRING,
    value        DOUBLE,
    unit         STRING,
    is_alert     BOOLEAN,
    ts           TIMESTAMP,
    dy           INT,
    hr           INT
)
PARTITIONED BY (warehouse_id STRING, yr INT, mn INT)
CLUSTERED BY (sensor_id) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true', 'orc.compress'='SNAPPY');

SHOW TABLES;
