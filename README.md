# Databricks Bronze–Silver–Gold (CSV → Delta) — Student Demo

End-to-end mini pipeline on Databricks using **3 CSVs** and **Unity Catalog Volumes**:
- **Bronze:** raw ingest (appendable) as Delta by path  
- **Silver:** cleaned/typed, keys standardized  
- **Gold:** business-ready KPI (avg amount per customer)  

> Works even with limited table/HC permissions by writing **Delta by path** under a Volume.

## Architecture (short)
- **Storage:** `/Volumes/<catalog>/<schema>/<volume>/...`
- **Compute:** Databricks cluster (any), Unity Catalog on
- **Format:** Delta Lake (ACID, time-travel, schema evolution)

## Quickstart (Databricks)
1. **Create/Use a Volume** (or ask admin):  
   `users.<your_email_sanitized>.raw_data`
2. **Upload files** (`sales.csv`, `products.csv`, `customers.csv`) into that Volume.
3. Open notebook `notebooks/bsg_csv_uc_volume.py` in Databricks and run **top to bottom**.
4. Outputs (Delta) appear under:  
   `/Volumes/users/<schema>/lakehouse_demo/{bronze,silver,gold}`.

> If you can’t create a Volume, ask for one to be created, or for a writable catalog+schema.

## What it delivers
- **Gold:** `avg_amount` per `CustomerID`
- **Data quality checks:** see `sql/quality_checks.sql`
- **Promotable:** switch from by-path Delta to UC tables when grants are available.

## Common fixes
- **Cannot use local filesystem** → Use `/Volumes/...` paths (this repo does).  
- **`samples` is read-only** → Don’t write there. Use `users` catalog or a team catalog.  
- **No permission to create schema/volume** → Ask admin; meanwhile, code still reads/writes by path once a Volume exists.

## License
MIT — see `LICENSE`.
