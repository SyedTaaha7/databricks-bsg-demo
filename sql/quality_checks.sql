-- Row counts & basic sanity
SELECT 
  COUNT(*) AS rows_total,
  SUM(CASE WHEN amount IS NULL OR amount <= 0 THEN 1 ELSE 0 END) AS bad_amounts,
  COUNT(DISTINCT CustomerID) AS unique_customers
FROM delta.`/Volumes/users/syedtaaha30_gmail_com/lakehouse_demo/silver/sales`;

-- Top customers by spend
SELECT CustomerID, SUM(amount) AS total_amount
FROM delta.`/Volumes/users/syedtaaha30_gmail_com/lakehouse_demo/silver/sales`
GROUP BY CustomerID
ORDER BY total_amount DESC
LIMIT 10;

-- Monthly average per customer (example)
SELECT
  CustomerID,
  date_trunc('month', to_timestamp(coalesce(event_ts, order_ts, purchase_ts, _ingest_ts))) AS month,
  AVG(amount) AS avg_amount
FROM delta.`/Volumes/users/syedtaaha30_gmail_com/lakehouse_demo/silver/sales`
GROUP BY CustomerID, date_trunc('month', to_timestamp(coalesce(event_ts, order_ts, purchase_ts, _ingest_ts)))
ORDER BY month DESC, avg_amount DESC;
