-- ============================================================
-- Script: 03_load_data.hql
-- Purpose: Load CSV data from staging into ORC tables
-- ============================================================

USE hive_analytics;

SET hive.exec.dynamic.partition      = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.enforce.bucketing           = true;

-- --------------------------------------------------------
-- Load Customers
-- --------------------------------------------------------
INSERT OVERWRITE TABLE dim_customers
PARTITION (region)
SELECT
    customer_id,
    name,
    email,
    age,
    gender,
    city,
    segment,
    loyalty_score,
    TO_DATE(join_date) AS join_date,
    region
FROM stg_customers;

-- --------------------------------------------------------
-- Load Products
-- --------------------------------------------------------
INSERT OVERWRITE TABLE dim_products
PARTITION (category)
SELECT
    product_id,
    name,
    sub_category,
    brand,
    base_price,
    cost_price,
    weight_kg,
    warehouse_id,
    stock_qty,
    rating,
    category
FROM stg_products;

-- --------------------------------------------------------
-- Load Transactions
-- --------------------------------------------------------
INSERT OVERWRITE TABLE fact_transactions
PARTITION (region, category, yr, mn)
SELECT
    t.transaction_id,
    t.customer_id,
    t.product_id,
    t.city,
    t.quantity,
    t.unit_price,
    t.discount,
    t.total_amount,
    t.payment_method,
    t.device_type,
    t.status,
    t.warehouse_id,
    TO_TIMESTAMP(t.ts, 'yyyy-MM-dd HH:mm:ss') AS ts,
    t.dy,
    t.hr,
    t.region,
    t.category,
    t.yr,
    t.mn
FROM stg_transactions t;

-- --------------------------------------------------------
-- Load Sensor Events
-- --------------------------------------------------------
INSERT OVERWRITE TABLE fact_sensor_events
PARTITION (warehouse_id, yr, mn)
SELECT
    event_id,
    sensor_id,
    sensor_type,
    value,
    unit,
    is_alert,
    TO_TIMESTAMP(ts, 'yyyy-MM-dd HH:mm:ss') AS ts,
    dy,
    hr,
    warehouse_id,
    yr,
    mn
FROM stg_sensor_events;

-- Verify row counts
SELECT 'dim_customers'     AS tbl, COUNT(*) AS rows FROM dim_customers     UNION ALL
SELECT 'dim_products'      AS tbl, COUNT(*) AS rows FROM dim_products      UNION ALL
SELECT 'fact_transactions' AS tbl, COUNT(*) AS rows FROM fact_transactions  UNION ALL
SELECT 'fact_sensor_events'AS tbl, COUNT(*) AS rows FROM fact_sensor_events;
