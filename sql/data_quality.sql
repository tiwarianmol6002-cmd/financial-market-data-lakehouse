-- Manual versions of the checks automated in databricks/04_quality_checks.py
SELECT COUNT(*) AS invalid_rows FROM finance_lakehouse.silver.stock_prices WHERE close <= 0;

SELECT ticker, trade_date, COUNT(*) AS duplicate_count
FROM finance_lakehouse.silver.stock_prices
GROUP BY ticker, trade_date
HAVING COUNT(*) > 1;

SELECT
  SUM(CASE WHEN ticker IS NULL THEN 1 ELSE 0 END)     AS null_ticker,
  SUM(CASE WHEN trade_date IS NULL THEN 1 ELSE 0 END) AS null_date,
  SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END)      AS null_close
FROM finance_lakehouse.silver.stock_prices;

-- Rows per ticker and date range (bronze sanity check)
SELECT ticker, COUNT(*) AS row_count, MIN(trade_date) AS first_date, MAX(trade_date) AS last_date
FROM finance_lakehouse.bronze.stock_prices
GROUP BY ticker ORDER BY ticker;
