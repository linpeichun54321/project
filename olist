import pandas as pd
import pyodbc
import os
import sys
import time
from math import ceil

# =========================
# 📁 路徑設定
# =========================
BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "dataset")

def check_file(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ 找不到檔案: {path}")

csv_files = {
    "orders": "olist_orders_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "products": "olist_products_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv"
}

# =========================
# 讀取 CSV
# =========================
data = {}
for key, filename in csv_files.items():
    path = os.path.join(DATA_DIR, filename)
    check_file(path)
    data[key] = pd.read_csv(path)

orders = data["orders"]
customers = data["customers"]
order_items = data["order_items"]
products = data["products"]
reviews = data["reviews"]

print("✅ 所有原始資料讀取完成")

# =========================
# SQL 連線（更新版）
# =========================
server = 'linpeichunhappy.database.windows.net'
database = 'lin_project'
username = 'missa'
password = 'Cc12345678'
driver = '{ODBC Driver 18 for SQL Server}'

conn = pyodbc.connect(
    f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
)
cursor = conn.cursor()
cursor.fast_executemany = True
print("✅ SQL 連線成功")

# =========================
# 安全轉字串（防爆）
# =========================
def safe_str(x):
    try:
        if pd.isna(x):
            return ""
        if isinstance(x, (int, float)) and abs(x) > 1e18:
            return ""
        return str(x)
    except:
        return ""

# =========================
# 重建表
# =========================
def recreate_table(table_name, df):
    cols = ", ".join([f"[{c}] NVARCHAR(MAX)" for c in df.columns])
    sql = f"""
    IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
        DROP TABLE {table_name};
    CREATE TABLE {table_name} ({cols});
    """
    cursor.execute(sql)
    conn.commit()

# =========================
# 分批寫入
# =========================
def insert_dataframe(df, table_name, batch_size=5000):
    df = df.copy()
    for col in df.columns:
        df[col] = df[col].apply(safe_str)

    cols = ", ".join([f"[{c}]" for c in df.columns])
    placeholders = ", ".join(["?"] * len(df.columns))

    total = len(df)
    start_time = time.time()

    for i in range(0, total, batch_size):
        batch = df.iloc[i:i+batch_size]
        rows = [tuple(x) for x in batch.to_numpy()]
        cursor.executemany(f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})", rows)
        conn.commit()

        done = min(i + batch_size, total)
        progress = done / total * 100
        elapsed = time.time() - start_time
        remain = elapsed / (done/total) - elapsed if done else 0

        sys.stdout.write(
            f"\r📥 {table_name}: {done}/{total} ({progress:.1f}%) | 剩餘: {time.strftime('%H:%M:%S', time.gmtime(remain))}"
        )
        sys.stdout.flush()
    print()

# =========================
# 1️⃣ Raw 層
# =========================
raw_tables = {
    "orders": "dbo.orders_raw",
    "customers": "dbo.customers_raw",
    "order_items": "dbo.order_items_raw",
    "products": "dbo.products_raw",
    "reviews": "dbo.reviews_raw"
}

for key, table in raw_tables.items():
    df = data[key]
    recreate_table(table, df)
    insert_dataframe(df, table)

print("✅ Raw 完成")

# =========================
# 2️⃣ Clean 層
# =========================
for df in [orders, customers, order_items, products, reviews]:
    df.columns = df.columns.str.lower()

orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'], errors='coerce')
orders['order_delivered_customer_date'] = pd.to_datetime(orders['order_delivered_customer_date'], errors='coerce')
orders['order_estimated_delivery_date'] = pd.to_datetime(orders['order_estimated_delivery_date'], errors='coerce')

reviews['review_creation_date'] = pd.to_datetime(reviews['review_creation_date'], errors='coerce')
reviews['review_answer_timestamp'] = pd.to_datetime(reviews['review_answer_timestamp'], errors='coerce')

orders = orders.dropna(subset=['order_id','customer_id'])
reviews = reviews.dropna(subset=['review_id','order_id','review_score'])

orders['delivery_days'] = (orders['order_delivered_customer_date'] - orders['order_purchase_timestamp']).dt.days
orders['is_late'] = (orders['order_delivered_customer_date'] > orders['order_estimated_delivery_date']).astype(int)

reviews['answer_days'] = (reviews['review_answer_timestamp'] - reviews['review_creation_date']).dt.days

clean_tables = {
    "orders": "dbo.orders_clean",
    "customers": "dbo.customers_clean",
    "order_items": "dbo.order_items_clean",
    "products": "dbo.products_clean",
    "reviews": "dbo.reviews_clean"
}

for key, table in clean_tables.items():
    df = eval(key)
    df.to_csv(os.path.join(DATA_DIR, f"{key}_clean.csv"), index=False)
    recreate_table(table, df)
    insert_dataframe(df, table)

print("✅ Clean 完成")

# =========================
# 3️⃣ 合併表
# =========================
order_full = order_items.merge(orders, on='order_id', how='left')
order_full = order_full.merge(products, on='product_id', how='left')
order_full = order_full.merge(customers, on='customer_id', how='left')
order_full = order_full.merge(reviews, on='order_id', how='left')

order_full.to_csv(os.path.join(DATA_DIR, "order_full.csv"), index=False)
table = "dbo.order_full"
recreate_table(table, order_full)
insert_dataframe(order_full, table)

print("🎉 ETL 三層全部完成！")
conn.close()