# Databricks notebook/source
# BRONZE → SILVER → GOLD (CSV-only) using Unity Catalog Volumes (no local file paths)

from pyspark.sql.functions import input_file_name, current_timestamp, col, trim
from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame

# === CONFIG: adjust if your org uses different names ===
CATALOG = "users"
SCHEMA  = "syedtaaha30_gmail_com"     # your email sanitized: syedtaaha30@gmail.com -> syedtaaha30_gmail_com
VOLUME  = "raw_data"                   # Volume where you uploaded CSVs

# Input CSVs you uploaded to the Volume
FILES = ["sales.csv", "products.csv", "customers.csv"]

# Derived paths
SRC_BASE  = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
OUT_BASE  = f"/Volumes/{CATALOG}/{SCHEMA}/lakehouse_demo"
bronze_dir = f"{OUT_BASE}/bronze"
silver_dir = f"{OUT_BASE}/silver"
gold_dir   = f"{OUT_BASE}/gold"

print("Reading from:", SRC_BASE)
print("Writing to  :", OUT_BASE)

# (Optional) set UC context; creating schema may require permissions (safe to try)
try:
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
    spark.sql(f"USE SCHEMA {SCHEMA}")
except Exception as e:
    print("Note: could not set UC context fully (non-blocking):", getattr(e, "desc", str(e)))

# ---------------- BRONZE ----------------
bronze_paths = {}
for fname in FILES:
    path = f"{SRC_BASE}/{fname}"
    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .csv(path)
          .withColumn("_source_file", input_file_name())
          .withColumn("_ingest_ts", current_timestamp()))
    out_path = f"{bronze_dir}/{fname.replace('.csv','')}"
    df.write.mode("overwrite").format("delta").save(out_path)
    bronze_paths[fname] = out_path
    print("Bronze written:", out_path)

# ---------------- helpers ----------------
def clean_strings(df: DataFrame) -> DataFrame:
    out = df
    for name, dtype in df.dtypes:
        if dtype == "string":
            out = out.withColumn(name, trim(col(name)))
    return out

def first_numeric(df: DataFrame):
    for n, t in df.dtypes:
        if t in ("int","bigint","long","float","double","decimal","smallint","tinyint"):
            return n
    return None

def pick_amount_column(df: DataFrame):
    preferred = ("amount","price","total","revenue","sales_value","value")
    for c in df.columns:
        if c.lower() in preferred: 
            return c
    return first_numeric(df)

def pick_customer_key(df: DataFrame):
    for c in df.columns:
        lc = c.lower()
        if "customer" in lc or lc in ("customerid","customer_id","user_id","client_id","buyer_id"):
            return c
    for c in df.columns:
        if "id" in c.lower(): 
            return c
    return df.columns[0]

# ---------------- SILVER (sales only to keep it minimal) ----------------
sales_bronze = spark.read.format("delta").load(bronze_paths["sales.csv"])
sales_silver = clean_strings(sales_bronze).dropDuplicates()

amount_col = pick_amount_column(sales_silver)
cust_col   = pick_customer_key(sales_silver)

if amount_col is None:
    raise ValueError("Couldn't infer a numeric amount column from sales.csv. Rename/add one (Amount/Price/Total/etc).")

if amount_col != "amount":
    sales_silver = sales_silver.withColumnRenamed(amount_col, "amount")
if cust_col != "CustomerID":
    sales_silver = sales_silver.withColumnRenamed(cust_col, "CustomerID")

sales_silver = sales_silver.withColumn("amount", col("amount").cast("double"))

silver_sales_out = f"{silver_dir}/sales"
sales_silver.write.mode("overwrite").format("delta").save(silver_sales_out)
print("Silver written:", silver_sales_out)
print("Detected columns -> CustomerID, amount")

# ---------------- GOLD ----------------
gold = (spark.read.format("delta").load(silver_sales_out)
        .groupBy("CustomerID")
        .agg({"amount": "avg"})
        .withColumnRenamed("avg(amount)", "avg_amount"))

gold_out = f"{gold_dir}/sales_summary"
gold.write.mode("overwrite").format("delta").save(gold_out)
print("Gold written:", gold_out)

# Nice preview for screenshots / demo
display(gold.orderBy(col("avg_amount").desc()))
