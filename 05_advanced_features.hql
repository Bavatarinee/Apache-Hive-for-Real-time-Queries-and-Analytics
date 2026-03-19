-- ============================================================
-- Script: 05_advanced_features.hql
-- Purpose: UDFs, VIEWs, TEZ optimizations, ACID operations
-- ============================================================

USE hive_analytics;

-- ============================================================
-- SECTION 1: Views for BI Tools
-- ============================================================

DROP VIEW IF EXISTS vw_monthly_summary;
CREATE VIEW vw_monthly_summary AS
SELECT
    yr,
    mn,
    region,
    category,
    COUNT(transaction_id)        AS orders,
    SUM(quantity)                AS units_sold,
    ROUND(SUM(total_amount), 2)  AS revenue,
    ROUND(AVG(total_amount), 2)  AS avg_order_value,
    COUNT(DISTINCT customer_id)  AS unique_customers
FROM fact_transactions
WHERE status = 'Completed'
GROUP BY yr, mn, region, category;

DROP VIEW IF EXISTS vw_realtime_alerts;
CREATE VIEW vw_realtime_alerts AS
SELECT
    e.warehouse_id,
    e.sensor_type,
    e.sensor_id,
    e.value,
    e.unit,
    e.ts,
    CASE
        WHEN e.sensor_type = 'Temperature' AND e.value > 40 THEN 'HIGH_TEMP'
        WHEN e.sensor_type = 'Temperature' AND e.value < -5 THEN 'LOW_TEMP'
        WHEN e.sensor_type = 'Humidity'    AND e.value > 85 THEN 'HIGH_HUMI'
        WHEN e.sensor_type = 'CO2'         AND e.value > 1500 THEN 'HIGH_CO2'
        WHEN e.sensor_type = 'Vibration'   AND e.value > 8  THEN 'HIGH_VIB'
        WHEN e.sensor_type = 'Pressure'    AND e.value < 950 THEN 'LOW_PRES'
        WHEN e.sensor_type = 'Pressure'    AND e.value > 1080 THEN 'HIGH_PRES'
        ELSE 'NORMAL'
    END AS alert_type
FROM fact_sensor_events e
WHERE e.is_alert = TRUE;

DROP VIEW IF EXISTS vw_customer_360;
CREATE VIEW vw_customer_360 AS
SELECT
    c.customer_id,
    c.name,
    c.email,
    c.age,
    c.gender,
    c.region,
    c.city,
    c.segment,  
    c.loyalty_score,
    c.join_date,
    COUNT(t.transaction_id)        AS total_orders,
    SUM(t.quantity)                AS total_items,
    ROUND(SUM(t.total_amount), 2)  AS lifetime_value,
    ROUND(AVG(t.total_amount), 2)  AS avg_order_value,
    MAX(t.ts)                      AS last_purchase,
    DATEDIFF(CURRENT_DATE, MAX(TO_DATE(t.ts))) AS days_since_last_purchase
FROM dim_customers c
LEFT JOIN fact_transactions t ON c.customer_id = t.customer_id AND t.status = 'Completed'
GROUP BY
    c.customer_id, c.name, c.email, c.age, c.gender,
    c.region, c.city, c.segment, c.loyalty_score, c.join_date;

-- ============================================================
-- SECTION 2: Performance Optimization Settings (Tez/LLAP)
-- ============================================================

-- Enable Tez execution engine
SET hive.execution.engine = tez;

-- Enable LLAP for interactive sub-second queries
SET hive.llap.execution.mode = all;

-- Vectorized execution (columnar ORC)
SET hive.vectorized.execution.enabled            = true;
SET hive.vectorized.execution.reduce.enabled     = true;

-- Cost-Based Optimizer
SET hive.cbo.enable                              = true;
SET hive.compute.query.using.stats              = true;
SET hive.stats.fetch.column.stats               = true;

-- Auto-convert joins to map-joins
SET hive.auto.convert.join                       = true;
SET hive.mapjoin.smalltable.filesize             = 25000000;

-- Partition pruning
SET hive.optimize.ppd                            = true;

-- Collect statistics after load
ANALYZE TABLE fact_transactions   COMPUTE STATISTICS;
ANALYZE TABLE fact_sensor_events  COMPUTE STATISTICS;
ANALYZE TABLE dim_customers       COMPUTE STATISTICS;
ANALYZE TABLE dim_products        COMPUTE STATISTICS;

-- ============================================================
-- SECTION 3: ACID Transactions (Merge / Upsert)
-- ============================================================

-- Simulate real-time upsert: update completed orders that were previously pending
MERGE INTO fact_transactions AS target
USING (
    SELECT transaction_id, 'Completed' AS new_status
    FROM fact_transactions
    WHERE status = 'Pending'
      AND ts < DATE_SUB(CURRENT_TIMESTAMP, 2)  -- pending for 2+ days => auto-complete
) AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED THEN UPDATE SET status = source.new_status;

-- ============================================================
-- SECTION 4: Incremental / Delta Load Pattern
-- ============================================================
-- (Run this nightly to load only new records)

CREATE TABLE IF NOT EXISTS incremental_watermark (
    table_name STRING,
    last_loaded_ts TIMESTAMP
)
STORED AS ORC;

-- Log watermark after each successful load
INSERT INTO incremental_watermark VALUES
    ('fact_transactions',   CURRENT_TIMESTAMP),
    ('fact_sensor_events',  CURRENT_TIMESTAMP);

-- ============================================================
-- SECTION 5: Data Quality Checks
-- ============================================================
SELECT 'Null transaction_ids' AS check_name, COUNT(*) AS bad_rows FROM fact_transactions WHERE transaction_id IS NULL
UNION ALL
SELECT 'Negative amounts',      COUNT(*) FROM fact_transactions WHERE total_amount < 0
UNION ALL
SELECT 'Future timestamps',     COUNT(*) FROM fact_transactions WHERE ts > CURRENT_TIMESTAMP
UNION ALL
SELECT 'Orphaned transactions', COUNT(*) FROM fact_transactions t
    LEFT JOIN dim_customers c ON t.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
UNION ALL
SELECT 'Sensor out-of-range CO2', COUNT(*) FROM fact_sensor_events WHERE sensor_type = 'CO2' AND value < 0;
