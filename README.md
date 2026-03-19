## 📌 Project Overview

This project simulates a real-world **e-commerce + IoT warehouse monitoring** platform processed entirely through **Apache Hive**. It covers the full pipeline:

```
Raw CSV Data → Hive Staging Tables → ORC Optimized Tables → Analytics Queries → Interactive Dashboard
```

---

## 📁 Project Structure

```
Apache Hive for Real-time Queries and Analytics/
│
├── dataset/
│   ├── generate_dataset.py       # Dataset generator (50K transactions, 30K sensor events)
│   ├── customers.csv             # 2,000 customer records
│   ├── products.csv              # 500 product records
│   ├── transactions.csv          # 50,000 e-commerce transactions
│   ├── sensor_events.csv         # 30,000 IoT warehouse sensor readings
│   └── analytics_data.json       # Pre-aggregated data for dashboard
│
├── hive_scripts/
│   ├── 01_create_database.hql    # Create Hive database
│   ├── 02_create_tables.hql      # Staging + ORC partitioned/bucketed tables
│   ├── 03_load_data.hql          # Dynamic partition loading
│   ├── 04_analytics_queries.hql  # 18 analytics queries
│   └── 05_advanced_features.hql  # Views, Tez/LLAP, ACID, data quality
│
├── dashboard/
│   ├── index.html                # Interactive analytics dashboard
│   ├── style.css                 # Dark-theme styling
│   └── app.js                    # Chart.js visualizations
│
├── aggregate_data.py             # Aggregates CSV → analytics_data.json

```

---

## 📊 Dataset

| Dataset         | Records | Description                                 |
|-----------------|--------:|---------------------------------------------|
| Customers       |   2,000 | Demographics, segment, loyalty score        |
| Products        |     500 | Category, pricing, stock, brand             |
| Transactions    |  50,000 | Orders, payments, discounts, timestamps     |
| Sensor Events   |  30,000 | IoT temperature, humidity, CO₂, vibration   |

**Key Metrics (2024)**
- 💰 **~$217M** total revenue (Completed orders)
- 🛒 **~37,500** completed orders
- 📦 **~206,500** units sold
- 🚨 **~5,860** sensor alerts

---

## 🐝 HiveQL Scripts

### 1. Database & Tables (`01`, `02`)
- External **staging tables** (CSV format) for raw ingestion
- **ORC** optimized tables with **SNAPPY** compression
- **Dynamic Partitioning** by region, category, year, month
- **Bucketing** (8–16 buckets) on customer/product IDs for fast joins

### 2. Data Loading (`03`)
- Dynamic partition INSERT from staging → ORC
- Full row count validation

### 3. Analytics Queries (`04`) — 18 Queries
| # | Query |
|---|-------|
| Q1 | Revenue KPIs — total orders, revenue, AOV |
| Q2 | Monthly revenue trend |
| Q3 | Revenue by category with discount analysis |
| Q4 | Revenue by region and top cities |
| Q5 | Payment method distribution with window % |
| Q6 | Device type analysis |
| Q7 | Customer segment performance |
| Q8 | Top 10 best-selling products |
| Q9 | Hourly order volume (peak hours) |
| Q10 | Order status distribution |
| Q11 | Warehouse performance + sensor joins |
| Q12 | Customer cohort by join year |
| Q13 | Discount impact analysis |
| Q14 | Sensor alerts by warehouse & type |
| Q15 | **Real-time micro-batch (5-min windows)** |
| Q16 | **Cumulative running revenue (Window function)** |
| Q17 | **Customer Lifetime Value (CLV) — Top 15** |
| Q18 | **Quarter-over-Quarter Growth (QoQ)** |

### 4. Advanced Features (`05`)
- **Views**: `vw_monthly_summary`, `vw_realtime_alerts`, `vw_customer_360`
- **Performance**: Tez engine, LLAP, vectorized execution, CBO, map-joins
- **ACID**: `MERGE INTO` for real-time upsert/update
- **Incremental Load**: Watermark-based delta load pattern
- **Data Quality**: 5 validation checks

---

## 🌐 Interactive Dashboard

The dashboard has **7 sections**:

| Section | Charts |
|---------|--------|
| Overview | KPI cards, monthly trend, order status, device, payment, hourly |
| Revenue | Dual-axis chart, cumulative total, discount analysis, warehouse revenue |
| Categories | Bar + pie charts, ranked summary table |
| Geography | Regional revenue bar + doughnut |
| Customers | Segment doughnut, trend lines, per-segment KPIs |
| IoT Sensors | Alert counts, radar chart, warehouse × type heatmap |
| HiveQL | Expandable syntax-highlighted query reference |

---

## 🚀 Quick Start

### Step 1 — Generate Dataset
```bash
cd dataset
python generate_dataset.py
```

### Step 2 — Aggregate for Dashboard
```bash
# From project root
python aggregate_data.py
```

### Step 3 — Launch Dashboard
```bash
cd dashboard
python -m http.server 8080
# Open http://localhost:8080
```

### Step 4 — Run on Apache Hive (requires Hadoop/Hive cluster)
```bash
hive -f hive_scripts/01_create_database.hql
hive -f hive_scripts/02_create_tables.hql
# Upload CSVs to HDFS first:
# hdfs dfs -put dataset/*.csv /user/hive/warehouse/hive_analytics/stg_*/
hive -f hive_scripts/03_load_data.hql
hive -f hive_scripts/04_analytics_queries.hql
hive -f hive_scripts/05_advanced_features.hql
```

---

## 🛠️ Technologies

| Technology | Purpose |
|------------|---------|
| **Apache Hive** | SQL-on-Hadoop, batch & interactive queries |
| **ORC Format** | Columnar storage, 3–5× faster reads |
| **Apache Tez** | DAG execution engine (replaces MapReduce) |
| **LLAP** | Sub-second interactive queries |
| **ACID Transactions** | Row-level updates/deletes/merges |
| **Window Functions** | Running totals, LAG, RANK, QoQ growth |
| **Python** | Dataset generation and aggregation |
| **Chart.js** | Interactive dashboard visualizations |

---
##Project Demo 

<img width="1910" height="871" alt="image" src="https://github.com/user-attachments/assets/1803d68e-7dea-4cc6-a7b4-73b2fedb9ea0" />


## 📖 Key Concepts Demonstrated

- ✅ **Partitioning** — Prune data by year/month/region for faster scans
- ✅ **Bucketing** — Enable efficient map-joins and sorted joins
- ✅ **ORC + Snappy** — Columnar compression for analytics workloads
- ✅ **Dynamic Partition Insert** — Scalable ETL pipelines
- ✅ **Window Functions** — LAG, SUM OVER, cumulative aggregations
- ✅ **ACID Merge** — Real-time upsert capability in Hive
- ✅ **Micro-batch Streaming** — 5-minute window aggregations (Hive + Kafka pattern)
- ✅ **Data Quality** — Automated validation queries
- ✅ **Cost-Based Optimizer (CBO)** — Statistics-driven query planning

---

*Built with 🐝 Apache Hive | Dataset: 82,500 synthetic records | 2024*
