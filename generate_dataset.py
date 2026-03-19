"""
Apache Hive Real-time Analytics Dataset Generator
Generates realistic e-commerce transaction and sensor event datasets
"""

import csv
import random
import os
from datetime import datetime, timedelta

random.seed(42)

# --- Configuration ---
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
START_DATE = datetime(2024, 1, 1)
END_DATE   = datetime(2024, 12, 31)
NUM_TRANSACTIONS  = 50000
NUM_SENSOR_EVENTS = 30000
NUM_CUSTOMERS     = 2000
NUM_PRODUCTS      = 500

# --- Reference Data ---
CATEGORIES     = ["Electronics", "Clothing", "Home & Kitchen", "Sports", "Books", "Toys", "Beauty", "Automotive", "Grocery", "Jewelry"]
REGIONS        = ["North America", "Europe", "Asia Pacific", "Latin America", "Middle East", "Africa"]
CITIES         = {
    "North America": ["New York", "Los Angeles", "Chicago", "Houston", "Toronto", "Vancouver"],
    "Europe":        ["London", "Paris", "Berlin", "Amsterdam", "Madrid", "Rome"],
    "Asia Pacific":  ["Tokyo", "Singapore", "Sydney", "Mumbai", "Shanghai", "Seoul"],
    "Latin America": ["São Paulo", "Mexico City", "Buenos Aires", "Bogotá", "Lima", "Santiago"],
    "Middle East":   ["Dubai", "Riyadh", "Tel Aviv", "Istanbul", "Cairo", "Doha"],
    "Africa":        ["Lagos", "Nairobi", "Cairo", "Johannesburg", "Accra", "Casablanca"],
}
PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Crypto", "Bank Transfer", "Gift Card"]
DEVICE_TYPES    = ["Mobile", "Desktop", "Tablet"]
STATUSES        = ["Completed", "Pending", "Refunded", "Cancelled", "Processing"]
STATUS_WEIGHTS  = [0.75, 0.10, 0.06, 0.05, 0.04]
SENSOR_TYPES    = ["Temperature", "Humidity", "Pressure", "Vibration", "Light", "CO2"]
WAREHOUSES      = ["WH-001", "WH-002", "WH-003", "WH-004", "WH-005"]

def random_date(start, end):
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def generate_customers(n):
    customers = []
    for i in range(1, n + 1):
        region = random.choice(REGIONS)
        city = random.choice(CITIES[region])
        age = random.randint(18, 75)
        segment = "Premium" if age > 40 and random.random() < 0.35 else ("Regular" if random.random() < 0.6 else "New")
        customers.append({
            "customer_id":   f"CUST-{i:05d}",
            "name":          f"Customer_{i}",
            "email":         f"customer{i}@example.com",
            "age":           age,
            "gender":        random.choice(["Male", "Female", "Other"]),
            "region":        region,
            "city":          city,
            "segment":       segment,
            "loyalty_score": round(random.uniform(1.0, 10.0), 2),
            "join_date":     random_date(datetime(2018, 1, 1), START_DATE).strftime("%Y-%m-%d"),
        })
    return customers

def generate_products(n):
    products = []
    for i in range(1, n + 1):
        category = random.choice(CATEGORIES)
        base_price = round(random.uniform(5.0, 2500.0), 2)
        products.append({
            "product_id":   f"PROD-{i:05d}",
            "name":         f"{category}_Product_{i}",
            "category":     category,
            "sub_category": f"{category}_Sub_{random.randint(1, 5)}",
            "brand":        f"Brand_{random.randint(1, 50)}",
            "base_price":   base_price,
            "cost_price":   round(base_price * random.uniform(0.3, 0.65), 2),
            "weight_kg":    round(random.uniform(0.1, 25.0), 2),
            "warehouse_id": random.choice(WAREHOUSES),
            "stock_qty":    random.randint(0, 5000),
            "rating":       round(random.uniform(3.0, 5.0), 2),
        })
    return products

def generate_transactions(customers, products, n):
    transactions = []
    customer_list = customers
    product_list  = products
    for i in range(1, n + 1):
        customer = random.choice(customer_list)
        product  = random.choice(product_list)
        qty      = random.randint(1, 10)
        disc     = round(random.uniform(0, 0.35), 2)
        unit_price = round(product["base_price"] * (1 - disc), 2)
        total = round(unit_price * qty, 2)
        ts = random_date(START_DATE, END_DATE)
        status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
        transactions.append({
            "transaction_id":  f"TXN-{i:07d}",
            "customer_id":     customer["customer_id"],
            "product_id":      product["product_id"],
            "category":        product["category"],
            "region":          customer["region"],
            "city":            customer["city"],
            "quantity":        qty,
            "unit_price":      unit_price,
            "discount":        disc,
            "total_amount":    total,
            "payment_method":  random.choice(PAYMENT_METHODS),
            "device_type":     random.choice(DEVICE_TYPES),
            "status":          status,
            "warehouse_id":    product["warehouse_id"],
            "timestamp":       ts.strftime("%Y-%m-%d %H:%M:%S"),
            "year":            ts.year,
            "month":           ts.month,
            "day":             ts.day,
            "hour":            ts.hour,
        })
    return transactions

def generate_sensor_events(n):
    events = []
    for i in range(1, n + 1):
        sensor_type = random.choice(SENSOR_TYPES)
        warehouse   = random.choice(WAREHOUSES)
        ts = random_date(START_DATE, END_DATE)
        if sensor_type == "Temperature":
            value = round(random.uniform(-10, 45), 2)
            unit  = "Celsius"
            alert = value > 40 or value < -5
        elif sensor_type == "Humidity":
            value = round(random.uniform(20, 95), 2)
            unit  = "%"
            alert = value > 85
        elif sensor_type == "Pressure":
            value = round(random.uniform(900, 1100), 2)
            unit  = "hPa"
            alert = value < 950 or value > 1080
        elif sensor_type == "Vibration":
            value = round(random.uniform(0, 10), 3)
            unit  = "mm/s"
            alert = value > 8
        elif sensor_type == "Light":
            value = round(random.uniform(0, 1000), 1)
            unit  = "Lux"
            alert = False
        else:  # CO2
            value = round(random.uniform(300, 2000), 1)
            unit  = "ppm"
            alert = value > 1500
        events.append({
            "event_id":     f"EVT-{i:07d}",
            "sensor_id":    f"SENS-{random.randint(1, 200):04d}",
            "sensor_type":  sensor_type,
            "warehouse_id": warehouse,
            "value":        value,
            "unit":         unit,
            "is_alert":     alert,
            "timestamp":    ts.strftime("%Y-%m-%d %H:%M:%S"),
            "year":         ts.year,
            "month":        ts.month,
            "day":          ts.day,
            "hour":         ts.hour,
        })
    return events

def write_csv(filename, data, fieldnames):
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"  ✅ Written {len(data):,} rows -> {filename}")

if __name__ == "__main__":
    print("🐝 Apache Hive Analytics Dataset Generator")
    print("=" * 50)

    print("\n📋 Generating customers ...")
    customers = generate_customers(NUM_CUSTOMERS)
    write_csv("customers.csv", customers, list(customers[0].keys()))

    print("\n📦 Generating products ...")
    products = generate_products(NUM_PRODUCTS)
    write_csv("products.csv", products, list(products[0].keys()))

    print("\n💳 Generating transactions ...")
    transactions = generate_transactions(customers, products, NUM_TRANSACTIONS)
    write_csv("transactions.csv", transactions, list(transactions[0].keys()))

    print("\n🌡️  Generating sensor events ...")
    sensors = generate_sensor_events(NUM_SENSOR_EVENTS)
    write_csv("sensor_events.csv", sensors, list(sensors[0].keys()))

    print("\n✨ Dataset generation complete!")
    print(f"   Customers:    {NUM_CUSTOMERS:,}")
    print(f"   Products:     {NUM_PRODUCTS:,}")
    print(f"   Transactions: {NUM_TRANSACTIONS:,}")
    print(f"   Sensor Events:{NUM_SENSOR_EVENTS:,}")
