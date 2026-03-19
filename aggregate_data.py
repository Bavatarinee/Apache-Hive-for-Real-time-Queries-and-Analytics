"""
Aggregates CSV data into analytics_data.json for the dashboard
"""
import csv, json
from collections import defaultdict

BASE = r"dataset"

def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

txns    = read_csv(f"{BASE}/transactions.csv")
sensors = read_csv(f"{BASE}/sensor_events.csv")
custs   = read_csv(f"{BASE}/customers.csv")
prods   = read_csv(f"{BASE}/products.csv")

# KPIs
completed = [t for t in txns if t["status"] == "Completed"]
total_rev  = round(sum(float(t["total_amount"]) for t in completed), 2)
total_orders = len(completed)
unique_cust  = len(set(t["customer_id"] for t in completed))
aov          = round(total_rev / total_orders, 2)
units_sold   = sum(int(t["quantity"]) for t in completed)
refunds      = len([t for t in txns if t["status"] == "Refunded"])
total_alerts = len([s for s in sensors if s["is_alert"].lower() == "true"])

# Monthly revenue
monthly_rev = defaultdict(float)
monthly_ord = defaultdict(int)
for t in completed:
    key = f"{t['year']}-{int(t['month']):02d}"
    monthly_rev[key] += float(t["total_amount"])
    monthly_ord[key] += 1
monthly_data = sorted(
    [{"month": k, "revenue": round(monthly_rev[k], 2), "orders": monthly_ord[k]}
     for k in monthly_rev],
    key=lambda x: x["month"]
)

# Category
cat_rev = defaultdict(float)
cat_cnt = defaultdict(int)
cat_units = defaultdict(int)
for t in completed:
    cat_rev[t["category"]]   += float(t["total_amount"])
    cat_cnt[t["category"]]   += 1
    cat_units[t["category"]] += int(t["quantity"])
cat_data = sorted(
    [{"category": k, "revenue": round(cat_rev[k], 2), "orders": cat_cnt[k], "units": cat_units[k]}
     for k in cat_rev],
    key=lambda x: -x["revenue"]
)

# Region
reg_rev = defaultdict(float)
reg_cnt = defaultdict(int)
for t in completed:
    reg_rev[t["region"]] += float(t["total_amount"])
    reg_cnt[t["region"]] += 1
reg_data = sorted(
    [{"region": k, "revenue": round(reg_rev[k], 2), "orders": reg_cnt[k]}
     for k in reg_rev],
    key=lambda x: -x["revenue"]
)

# Payment methods
pay_rev = defaultdict(float)
pay_cnt = defaultdict(int)
for t in completed:
    pay_rev[t["payment_method"]] += float(t["total_amount"])
    pay_cnt[t["payment_method"]] += 1
pay_data = sorted(
    [{"method": k, "revenue": round(pay_rev[k], 2), "orders": pay_cnt[k]}
     for k in pay_rev],
    key=lambda x: -x["revenue"]
)

# Device types
dev_cnt = defaultdict(int)
dev_rev = defaultdict(float)
for t in completed:
    dev_cnt[t["device_type"]] += 1
    dev_rev[t["device_type"]] += float(t["total_amount"])
dev_data = [{"device": k, "orders": dev_cnt[k], "revenue": round(dev_rev[k], 2)} for k in dev_cnt]

# Hourly
hourly_cnt = defaultdict(int)
hourly_rev = defaultdict(float)
for t in completed:
    hourly_cnt[int(t["hour"])] += 1
    hourly_rev[int(t["hour"])] += float(t["total_amount"])
hourly_data = [{"hour": h, "orders": hourly_cnt[h], "revenue": round(hourly_rev[h], 2)}
               for h in sorted(hourly_cnt.keys())]

# Status
status_cnt = defaultdict(int)
status_val = defaultdict(float)
for t in txns:
    status_cnt[t["status"]] += 1
    status_val[t["status"]] += float(t["total_amount"])
status_data = [{"status": k, "count": status_cnt[k], "value": round(status_val[k], 2)}
               for k in status_cnt]

# Sensor summary by type
st_total = defaultdict(lambda: {"total": 0, "alerts": 0, "vals": []})
for s in sensors:
    st = s["sensor_type"]
    st_total[st]["total"]  += 1
    if s["is_alert"].lower() == "true":
        st_total[st]["alerts"] += 1
    st_total[st]["vals"].append(float(s["value"]))
sensor_summary = [
    {"type": k, "total": v["total"], "alerts": v["alerts"],
     "avg_val": round(sum(v["vals"]) / len(v["vals"]), 2)}
    for k, v in st_total.items()
]

# Warehouse sensor alerts
wh_alerts = defaultdict(lambda: defaultdict(int))
wh_total  = defaultdict(lambda: defaultdict(int))
for s in sensors:
    wh_total[s["warehouse_id"]][s["sensor_type"]] += 1
    if s["is_alert"].lower() == "true":
        wh_alerts[s["warehouse_id"]][s["sensor_type"]] += 1
wh_alert_data = []
for wh in sorted(wh_total.keys()):
    for st in sorted(wh_total[wh].keys()):
        wh_alert_data.append({
            "warehouse": wh, "sensor_type": st,
            "total": wh_total[wh][st], "alerts": wh_alerts[wh][st]
        })

# Warehouse revenue
wh_rev = defaultdict(float)
wh_ord = defaultdict(int)
for t in completed:
    wh_rev[t["warehouse_id"]] += float(t["total_amount"])
    wh_ord[t["warehouse_id"]] += 1
wh_data = sorted([{"warehouse": k, "revenue": round(wh_rev[k], 2), "orders": wh_ord[k]}
                  for k in wh_rev], key=lambda x: -x["revenue"])

# Customer segments
seg_rev = defaultdict(float)
seg_cnt = defaultdict(int)
for t in completed:
    cust = next((c for c in custs if c["customer_id"] == t["customer_id"]), None)
    if cust:
        seg_rev[cust["segment"]] += float(t["total_amount"])
        seg_cnt[cust["segment"]] += 1
seg_data = [{"segment": k, "revenue": round(seg_rev[k], 2), "orders": seg_cnt[k]} for k in seg_rev]

# Discount buckets
disc_buckets = {"No Discount": 0, "1-10%": 0, "11-20%": 0, "21-30%": 0, "31%+": 0}
disc_rev     = {"No Discount": 0.0, "1-10%": 0.0, "11-20%": 0.0, "21-30%": 0.0, "31%+": 0.0}
for t in completed:
    d = float(t["discount"])
    if d == 0: b = "No Discount"
    elif d <= 0.10: b = "1-10%"
    elif d <= 0.20: b = "11-20%"
    elif d <= 0.30: b = "21-30%"
    else:           b = "31%+"
    disc_buckets[b] += 1
    disc_rev[b]     += float(t["total_amount"])
disc_data = [{"bucket": k, "orders": disc_buckets[k], "revenue": round(disc_rev[k], 2)} for k in disc_buckets]

result = {
    "kpis": {
        "total_revenue": total_rev,
        "total_orders": total_orders,
        "unique_customers": unique_cust,
        "aov": aov,
        "units_sold": units_sold,
        "refunds": refunds,
        "total_alerts": total_alerts
    },
    "monthly": monthly_data,
    "categories": cat_data,
    "regions": reg_data,
    "payment": pay_data,
    "devices": dev_data,
    "hourly": hourly_data,
    "status": status_data,
    "warehouse_alerts": wh_alert_data,
    "sensor_summary": sensor_summary,
    "warehouses": wh_data,
    "segments": seg_data,
    "discount_buckets": disc_data
}

out = r"dataset/analytics_data.json"
with open(out, "w") as f:
    json.dump(result, f)

print(f"analytics_data.json written.")
print(f"  Revenue      : ${total_rev:,.2f}")
print(f"  Orders       : {total_orders:,}")
print(f"  Customers    : {unique_cust:,}")
print(f"  AOV          : ${aov:,.2f}")
print(f"  Units Sold   : {units_sold:,}")
print(f"  Sensor Alerts: {total_alerts:,}")
