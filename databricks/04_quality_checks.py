# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Data-quality checks
# MAGIC Fails the job (raises) when a rule is violated, so bad data never goes unnoticed.

# COMMAND ----------

dbutils.widgets.text("catalog", "finance_lakehouse")
C = dbutils.widgets.get("catalog")

# Each check is a query returning the number of offending rows; expected result is 0.
CHECKS = {
    "silver.stock_prices: non-positive close":
        f"SELECT COUNT(*) FROM {C}.silver.stock_prices WHERE close <= 0",
    "silver.stock_prices: duplicate ticker/date":
        f"SELECT COUNT(*) FROM (SELECT ticker, trade_date FROM {C}.silver.stock_prices GROUP BY 1,2 HAVING COUNT(*) > 1)",
    "silver.stock_prices: null key/close":
        f"SELECT COUNT(*) FROM {C}.silver.stock_prices WHERE ticker IS NULL OR trade_date IS NULL OR close IS NULL",
    "silver.stock_prices: high < low":
        f"SELECT COUNT(*) FROM {C}.silver.stock_prices WHERE high < low",
    "silver.stock_prices: negative volume":
        f"SELECT COUNT(*) FROM {C}.silver.stock_prices WHERE volume < 0",
    "silver.company_metadata: duplicate ticker":
        f"SELECT COUNT(*) FROM (SELECT ticker FROM {C}.silver.company_metadata GROUP BY 1 HAVING COUNT(*) > 1)",
    "silver.financials: duplicate key":
        f"SELECT COUNT(*) FROM (SELECT 1 FROM {C}.silver.financials GROUP BY ticker, report_date, statement_type, line_item HAVING COUNT(*) > 1)",
    "gold.daily_returns: extreme daily return (>|50%|)":
        f"SELECT COUNT(*) FROM {C}.gold.daily_returns WHERE ABS(daily_return) > 0.5",
    "gold.daily_returns: negative volatility":
        f"SELECT COUNT(*) FROM {C}.gold.daily_returns WHERE volatility_30d < 0",
    "gold: tickers missing from sector_performance join":
        f"SELECT COUNT(*) FROM {C}.gold.sector_performance WHERE sector = 'Unknown'",
    "gold.company_fundamentals: empty":
        f"SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM {C}.gold.company_fundamentals",
    "silver.stock_prices: fewer than 5 tickers":
        f"SELECT CASE WHEN COUNT(DISTINCT ticker) < 5 THEN 1 ELSE 0 END FROM {C}.silver.stock_prices",
}

# COMMAND ----------

failed = []
for name, query in CHECKS.items():
    bad = spark.sql(query).first()[0]
    status = "PASS" if bad == 0 else "FAIL"
    print(f"[{status}] {name} -> {bad}")
    if bad != 0:
        failed.append(name)

if failed:
    raise Exception(f"{len(failed)} data-quality check(s) failed: {failed}")
print("All data-quality checks passed.")
