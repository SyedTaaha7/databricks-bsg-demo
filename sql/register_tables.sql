-- Promote Silver/Gold Delta paths to UC tables
CREATE TABLE IF NOT EXISTS silver_sales
USING DELTA
LOCATION '/Volumes/users/syedtaaha30_gmail_com/lakehouse_demo/silver/sales';

CREATE TABLE IF NOT EXISTS gold_sales_summary
USING DELTA
LOCATION '/Volumes/users/syedtaaha30_gmail_com/lakehouse_demo/gold/sales_summary';
