# Lakehouse Architecture — CSV ➜ Bronze / Silver / Gold (Delta on Unity Catalog Volumes)

This document explains the tiny demo pipeline that turns three CSVs into reliable Delta datasets using the Lakehouse layering pattern.

---

## 1) High-level overview


**Why layers?**
- **Bronze** preserves raw fidelity and makes reprocessing easy.  
- **Silver** enforces schema & data quality so downstream users trust the data.  
- **Gold** serves consumable analytics (tables/charts/BI) with clear definitions.

---

## 2) Storage & paths (Unity Catalog Volumes)

All I/O uses UC **Volumes** (no local `file:/Workspace`).

- **Catalog:** `users`
- **Schema:** `syedtaaha30_gmail_com`  *(sanitized from `syedtaaha30@gmail.com`)*
- **Volume (input):** `raw_data`

**Input CSVs**

**Delta outputs**

---

## 3) Data flow (what each layer does)

### Bronze (ingest)
- Read each CSV with header and `inferSchema=true`.
- Add audit columns: `_source_file`, `_ingest_ts`.
- Write **Delta** by path (one folder per dataset).
- No business transformations here.

### Silver (standardize)
- Start with `sales` only (keep the demo minimal).
- Clean strings (trim), **deduplicate** rows.
- Find a numeric measure column (e.g., `amount/price/total`); cast to **double** as `amount`.
- Detect customer key column (e.g., `customer_id`); rename to **`CustomerID`**.
- Enforce basic rules: non-null keys, amount is numeric.

### Gold (serve)
- Aggregate **avg spend per customer**:
  - `groupBy(CustomerID).avg(amount) ➜ avg_amount`
- Save as **Delta** under `gold/sales_summary`.

---

## 4) Logical model

**Inputs**
- `sales.csv`: at least `{ customer_id?, product_id?, amount?, ts? }`
- `products.csv`: `{ product_id, ... }`
- `customers.csv`: `{ customer_id, ... }`

**Silver (current demo)**
- `sales (Delta)`: `{ CustomerID: string, amount: double, …, _ingest_ts }`

**Gold**
- `sales_summary (Delta)`: `{ CustomerID: string, avg_amount: double }`

> Note: Extend Silver to standardize types/keys in `products` & `customers`, then join in Gold for category/customer segments.

---

## 5) Data quality & checks (examples)

**Row counts & bad values**
```sql
SELECT 
  COUNT(*) AS rows_total,
  SUM(CASE WHEN amount IS NULL OR amount <= 0 THEN 1 ELSE 0 END) AS bad_amounts,
  COUNT(DISTINCT CustomerID) AS unique_customers
FROM delta.`/Volumes/users/syedtaaha30_gmail_com/lakehouse_demo/silver/sales`;
SELECT CustomerID, SUM(amount) AS total_amount
FROM delta.`/Volumes/users/syedtaaha30_gmail_com/lakehouse_demo/silver/sales`
GROUP BY CustomerID
ORDER BY total_amount DESC
LIMIT 10;
SELECT
  CustomerID,
  date_trunc('month', to_timestamp(coalesce(event_ts, order_ts, purchase_ts, _ingest_ts))) AS month,
  AVG(amount) AS avg_amount
FROM delta.`/Volumes/users/syedtaaha30_gmail_com/lakehouse_demo/silver/sales`
GROUP BY CustomerID, date_trunc('month', to_timestamp(coalesce(event_ts, order_ts, purchase_ts, _ingest_ts)))
ORDER BY month DESC, avg_amount DESC;


