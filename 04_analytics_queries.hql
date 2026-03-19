-- ============================================================
-- Script: 04_analytics_queries.hql
-- Purpose: Core analytics queries for real-time dashboards
-- ============================================================

USE hive_analytics;

-- ============================================================
-- Q1: Revenue Overview - Total KPIs
-- ============================================================
SELECT
    COUNT(DISTINCT transaction_id)                     AS total_orders,
    COUNT(DISTINCT customer_id)                        AS unique_customers,
    ROUND(SUM(total_amount), 2)                        AS total_revenue,
    ROUND(AVG(total_amount), 2)                        AS avg_order_value,
    ROUND(SUM(quantity), 0)                            AS total_units_sold,
    ROUND(SUM(total_amount) / COUNT(DISTINCT customer_id), 2) AS revenue_per_customer
FROM fact_transactions
WHERE status = 'Completed';


-- ============================================================
-- Q2: Monthly Revenue Trend (2024)
-- ============================================================
SELECT
    yr,
    mn,
    ROUND(SUM(total_amount), 2)  AS monthly_revenue,
    COUNT(transaction_id)         AS orders,
    ROUND(AVG(total_amount), 2)   AS avg_order_value
FROM fact_transactions
WHERE status = 'Completed'
  AND yr = 2024
GROUP BY yr, mn
ORDER BY yr, mn;


-- ============================================================
-- Q3: Revenue by Category (descending)
-- ============================================================
SELECT
    category,
    COUNT(transaction_id)            AS total_orders,
    SUM(quantity)                    AS units_sold,
    ROUND(SUM(total_amount), 2)      AS revenue,
    ROUND(AVG(unit_price), 2)        AS avg_price,
    ROUND(AVG(discount) * 100, 2)   AS avg_discount_pct
FROM fact_transactions
WHERE status = 'Completed'
GROUP BY category
ORDER BY revenue DESC;


-- ============================================================
-- Q4: Revenue by Region and City (Top 20 cities)
-- ============================================================
SELECT
    region,
    city,
    COUNT(transaction_id)        AS orders,
    ROUND(SUM(total_amount), 2)  AS revenue,
    ROUND(AVG(total_amount), 2)  AS avg_order
FROM fact_transactions
WHERE status = 'Completed'
GROUP BY region, city
ORDER BY revenue DESC
LIMIT 20;


-- ============================================================
-- Q5: Payment Method Distribution
-- ============================================================
SELECT
    payment_method,
    COUNT(transaction_id)                          AS orders,
    ROUND(SUM(total_amount), 2)                    AS revenue,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_orders
FROM fact_transactions
WHERE status = 'Completed'
GROUP BY payment_method
ORDER BY revenue DESC;


-- ============================================================
-- Q6: Device Type Analysis
-- ============================================================
SELECT
    device_type,
    COUNT(transaction_id)        AS orders,
    ROUND(SUM(total_amount), 2)  AS revenue,
    ROUND(AVG(total_amount), 2)  AS avg_order_value
FROM fact_transactions
WHERE status = 'Completed'
GROUP BY device_type
ORDER BY revenue DESC;


-- ============================================================
-- Q7: Customer Segment Performance
-- ============================================================
SELECT
    c.segment,
    COUNT(DISTINCT t.customer_id)    AS customers,
    COUNT(t.transaction_id)          AS orders,
    ROUND(SUM(t.total_amount), 2)    AS revenue,
    ROUND(AVG(t.total_amount), 2)    AS avg_order,
    ROUND(AVG(c.loyalty_score), 2)   AS avg_loyalty
FROM fact_transactions t
JOIN dim_customers c ON t.customer_id = c.customer_id
WHERE t.status = 'Completed'
GROUP BY c.segment
ORDER BY revenue DESC;


-- ============================================================
-- Q8: Top 10 Best-Selling Products
-- ============================================================
SELECT
    p.product_id,
    p.name,
    p.category,
    p.brand,
    COUNT(t.transaction_id)      AS total_orders,
    SUM(t.quantity)              AS units_sold,
    ROUND(SUM(t.total_amount), 2) AS revenue,
    ROUND(p.rating, 2)           AS rating
FROM fact_transactions t
JOIN dim_products p ON t.product_id = p.product_id
WHERE t.status = 'Completed'
GROUP BY p.product_id, p.name, p.category, p.brand, p.rating
ORDER BY revenue DESC
LIMIT 10;


-- ============================================================
-- Q9: Hourly Order Volume (Peak Hours)
-- ============================================================
SELECT
    hr AS hour_of_day,
    COUNT(transaction_id)        AS orders,
    ROUND(SUM(total_amount), 2)  AS revenue
FROM fact_transactions
WHERE status = 'Completed'
GROUP BY hr
ORDER BY hr;


-- ============================================================
-- Q10: Order Status Distribution
-- ============================================================
SELECT
    status,
    COUNT(transaction_id)                          AS orders,
    ROUND(SUM(total_amount), 2)                    AS total_value,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM fact_transactions
GROUP BY status
ORDER BY orders DESC;


-- ============================================================
-- Q11: Warehouse Performance
-- ============================================================
SELECT
    t.warehouse_id,
    COUNT(DISTINCT t.transaction_id)  AS orders,
    SUM(t.quantity)                   AS units_dispatched,
    ROUND(SUM(t.total_amount), 2)     AS revenue,
    COUNT(DISTINCT s.event_id)        AS sensor_events,
    COUNT(DISTINCT CASE WHEN s.is_alert = TRUE THEN s.event_id END) AS alerts
FROM fact_transactions t
LEFT JOIN fact_sensor_events s ON t.warehouse_id = s.warehouse_id
WHERE t.status = 'Completed'
GROUP BY t.warehouse_id
ORDER BY revenue DESC;


-- ============================================================
-- Q12: Customer Cohort — Revenue by Join Year
-- ============================================================
SELECT
    YEAR(c.join_date) AS cohort_year,
    COUNT(DISTINCT c.customer_id)  AS customers,
    COUNT(t.transaction_id)        AS orders,
    ROUND(SUM(t.total_amount), 2)  AS revenue,
    ROUND(AVG(t.total_amount), 2)  AS avg_order
FROM fact_transactions t
JOIN dim_customers c ON t.customer_id = c.customer_id
WHERE t.status = 'Completed'
GROUP BY YEAR(c.join_date)
ORDER BY cohort_year;


-- ============================================================
-- Q13: Discount Impact on Revenue
-- ============================================================
SELECT
    CASE
        WHEN discount = 0              THEN 'No Discount'
        WHEN discount BETWEEN 0 AND 0.10 THEN '1-10%'
        WHEN discount BETWEEN 0.10 AND 0.20 THEN '11-20%'
        WHEN discount BETWEEN 0.20 AND 0.30 THEN '21-30%'
        ELSE '31%+'
    END AS discount_bucket,
    COUNT(transaction_id)        AS orders,
    SUM(quantity)                AS units_sold,
    ROUND(SUM(total_amount), 2)  AS revenue,
    ROUND(AVG(total_amount), 2)  AS avg_order
FROM fact_transactions
WHERE status = 'Completed'
GROUP BY
    CASE
        WHEN discount = 0              THEN 'No Discount'
        WHEN discount BETWEEN 0 AND 0.10 THEN '1-10%'
        WHEN discount BETWEEN 0.10 AND 0.20 THEN '11-20%'
        WHEN discount BETWEEN 0.20 AND 0.30 THEN '21-30%'
        ELSE '31%+'
    END
ORDER BY orders DESC;


-- ============================================================
-- Q14: Sensor Alerts by Warehouse and Type
-- ============================================================
SELECT
    warehouse_id,
    sensor_type,
    COUNT(event_id)                                          AS total_events,
    SUM(CAST(is_alert AS INT))                               AS alert_count,
    ROUND(SUM(CAST(is_alert AS INT)) * 100.0 / COUNT(*), 2) AS alert_rate_pct,
    ROUND(AVG(value), 2)                                     AS avg_value,
    ROUND(MIN(value), 2)                                     AS min_value,
    ROUND(MAX(value), 2)                                     AS max_value
FROM fact_sensor_events
GROUP BY warehouse_id, sensor_type
ORDER BY alert_count DESC;


-- ============================================================
-- Q15: Real-Time Streaming Simulation — Micro-Batch (last 1hr)
-- ============================================================
SELECT
    FROM_UNIXTIME(UNIX_TIMESTAMP(ts) - MOD(UNIX_TIMESTAMP(ts), 300),
                  'yyyy-MM-dd HH:mm') AS window_5min,
    COUNT(transaction_id)              AS orders,
    ROUND(SUM(total_amount), 2)        AS revenue
FROM fact_transactions
WHERE ts >= DATE_SUB(CURRENT_TIMESTAMP, 1)   -- last 1 hour window
  AND status = 'Completed'
GROUP BY FROM_UNIXTIME(UNIX_TIMESTAMP(ts) - MOD(UNIX_TIMESTAMP(ts), 300),
                       'yyyy-MM-dd HH:mm')
ORDER BY window_5min;


-- ============================================================
-- Q16: Running Cumulative Revenue (Window Function)
-- ============================================================
SELECT
    yr,
    mn,
    monthly_revenue,
    ROUND(SUM(monthly_revenue) OVER (ORDER BY yr, mn
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS cumulative_revenue
FROM (
    SELECT
        yr,
        mn,
        ROUND(SUM(total_amount), 2) AS monthly_revenue
    FROM fact_transactions
    WHERE status = 'Completed'
    GROUP BY yr, mn
) monthly
ORDER BY yr, mn;


-- ============================================================
-- Q17: Customer Lifetime Value (CLV) — Top 15
-- ============================================================
SELECT
    c.customer_id,
    c.name,
    c.segment,
    c.region,
    c.loyalty_score,
    COUNT(t.transaction_id)       AS total_orders,
    SUM(t.quantity)               AS units_bought,
    ROUND(SUM(t.total_amount), 2) AS lifetime_value,
    ROUND(AVG(t.total_amount), 2) AS avg_order
FROM fact_transactions t
JOIN dim_customers c ON t.customer_id = c.customer_id
WHERE t.status = 'Completed'
GROUP BY c.customer_id, c.name, c.segment, c.region, c.loyalty_score
ORDER BY lifetime_value DESC
LIMIT 15;


-- ============================================================
-- Q18: YoY / MoM Growth (Quarter Comparison)
-- ============================================================
SELECT
    yr,
    CEIL(mn / 3.0)                                       AS quarter,
    ROUND(SUM(total_amount), 2)                          AS revenue,
    LAG(ROUND(SUM(total_amount), 2), 1) OVER
        (ORDER BY yr, CEIL(mn / 3.0))                    AS prev_quarter_revenue,
    ROUND(
        (SUM(total_amount) - LAG(SUM(total_amount)) OVER
            (ORDER BY yr, CEIL(mn / 3.0))) /
        NULLIF(LAG(SUM(total_amount)) OVER
            (ORDER BY yr, CEIL(mn / 3.0)), 0) * 100, 2) AS qoq_growth_pct
FROM fact_transactions
WHERE status = 'Completed'
GROUP BY yr, CEIL(mn / 3.0)
ORDER BY yr, quarter;
