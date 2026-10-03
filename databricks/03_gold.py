# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: analytics-ready tables
# MAGIC `daily_returns`, `volatility_metrics`, `sector_performance`, `company_fundamentals`.
# MAGIC Returns use `adjusted_close` so dividends and splits do not create fake jumps.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "finance_lakehouse")
CATALOG = dbutils.widgets.get("catalog")
TRADING_DAYS = 252

def save(df, table):
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.gold.{table}"))
    print(f"gold.{table}: {spark.table(f'{CATALOG}.gold.{table}').count()} rows")

prices = spark.table(f"{CATALOG}.silver.stock_prices")
metadata = spark.table(f"{CATALOG}.silver.company_metadata")

# COMMAND ----------
# MAGIC %md ## 1. daily_returns

# COMMAND ----------

w = Window.partitionBy("ticker").orderBy("trade_date")
w20 = w.rowsBetween(-19, 0)
w30 = w.rowsBetween(-29, 0)
w50 = w.rowsBetween(-49, 0)
w_all = w.rowsBetween(Window.unboundedPreceding, 0)

df = (
    prices
    .withColumn("previous_close", F.lag("adjusted_close").over(w))
    .withColumn("daily_return", F.col("adjusted_close") / F.col("previous_close") - 1)
    .withColumn("cumulative_return", F.col("adjusted_close") / F.first("adjusted_close").over(w_all) - 1)
    # moving averages only once a full window exists
    .withColumn("ma_20", F.when(F.count("close").over(w20) == 20, F.avg("close").over(w20)))
    .withColumn("ma_50", F.when(F.count("close").over(w50) == 50, F.avg("close").over(w50)))
    .withColumn("volume_ma_20", F.when(F.count("volume").over(w20) == 20, F.avg("volume").over(w20)))
    .withColumn(
        "volatility_30d",
        F.when(F.count("daily_return").over(w30) == 30, F.stddev("daily_return").over(w30)),
    )
    .withColumn("annualized_volatility", F.col("volatility_30d") * F.sqrt(F.lit(float(TRADING_DAYS))))
    .withColumn("running_max", F.max("adjusted_close").over(w_all))
    .withColumn("drawdown", F.col("adjusted_close") / F.col("running_max") - 1)
    .withColumn(
        "trend_signal",
        F.when(F.col("ma_20").isNull() | F.col("ma_50").isNull(), F.lit(None).cast("string"))
         .when(F.col("ma_20") > F.col("ma_50"), "bullish")
         .otherwise("bearish"),
    )
)

daily_returns = df.select(
    "ticker", "trade_date", "close", "adjusted_close", "volume",
    "daily_return", "cumulative_return", "ma_20", "ma_50", "trend_signal",
    "volatility_30d", "annualized_volatility", "volume_ma_20", "drawdown",
)
save(daily_returns, "daily_returns")

# COMMAND ----------
# MAGIC %md ## 2. volatility_metrics (one row per ticker)

# COMMAND ----------

dr = spark.table(f"{CATALOG}.gold.daily_returns").filter(F.col("daily_return").isNotNull())
latest = dr.agg(F.max("trade_date")).first()[0]
one_year_ago = F.date_sub(F.lit(latest), 365)

volatility_metrics = (
    dr.groupBy("ticker")
    .agg(
        F.min("trade_date").alias("first_date"),
        F.max("trade_date").alias("last_date"),
        F.count("*").alias("trading_days"),
        F.avg("daily_return").alias("avg_daily_return"),
        (F.avg("daily_return") * TRADING_DAYS).alias("annualized_return"),
        (F.stddev("daily_return") * F.sqrt(F.lit(float(TRADING_DAYS)))).alias("annualized_volatility_full"),
        (F.stddev(F.when(F.col("trade_date") >= one_year_ago, F.col("daily_return")))
         * F.sqrt(F.lit(float(TRADING_DAYS)))).alias("annualized_volatility_1y"),
        F.min("drawdown").alias("max_drawdown"),
        F.max("cumulative_return").alias("peak_cumulative_return"),
        F.max_by("cumulative_return", "trade_date").alias("total_return"),
    )
    # simple Sharpe-style ratio with a 0% risk-free rate
    .withColumn("return_to_risk", F.col("annualized_return") / F.col("annualized_volatility_full"))
)
save(volatility_metrics, "volatility_metrics")

# COMMAND ----------
# MAGIC %md ## 3. sector_performance (equal-weighted, not market-cap weighted)

# COMMAND ----------

sector_daily = (
    spark.table(f"{CATALOG}.gold.daily_returns")
    .join(metadata.select("ticker", "sector"), "ticker", "left")
    .withColumn("sector", F.coalesce("sector", F.lit("Unknown")))
    .groupBy("trade_date", "sector")
    .agg(
        F.avg("daily_return").alias("average_daily_return"),
        F.avg("annualized_volatility").alias("average_volatility"),
        F.countDistinct("ticker").alias("num_stocks"),
    )
)

ws = Window.partitionBy("sector").orderBy("trade_date").rowsBetween(Window.unboundedPreceding, 0)
sector_performance = sector_daily.withColumn(
    # growth of 1 unit invested in an equal-weighted sector basket
    "sector_index",
    F.exp(F.sum(F.log1p(F.coalesce("average_daily_return", F.lit(0.0)))).over(ws)),
)
save(sector_performance, "sector_performance")

# COMMAND ----------
# MAGIC %md ## 4. company_fundamentals

# COMMAND ----------

LINE_ITEMS = {
    "revenue": ["Total Revenue", "Operating Revenue"],
    "net_income": ["Net Income", "Net Income Common Stockholders"],
    "total_assets": ["Total Assets"],
    "total_liabilities": ["Total Liabilities Net Minority Interest"],
    "total_debt": ["Total Debt"],
    "cash": ["Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments"],
    "equity": ["Stockholders Equity", "Common Stock Equity"],
}

fin = spark.table(f"{CATALOG}.silver.financials")
pivoted = (
    fin.groupBy("ticker", "report_date")
    .pivot("line_item", [name for names in LINE_ITEMS.values() for name in names])
    .agg(F.first("value"))
)
# coalesce alternative source names into one column per metric
for metric, names in LINE_ITEMS.items():
    existing = [F.col(f"`{n}`") for n in names if n in pivoted.columns]
    pivoted = pivoted.withColumn(metric, F.coalesce(*existing) if existing else F.lit(None).cast("double"))

fundamentals = (
    pivoted.select("ticker", "report_date", *LINE_ITEMS.keys())
    .join(metadata.select("ticker", "company_name", "sector", "industry", "market_cap"), "ticker", "left")
    .withColumn("net_margin", F.col("net_income") / F.col("revenue"))
    .withColumn("debt_to_equity", F.col("total_debt") / F.col("equity"))
    .withColumn("return_on_equity", F.col("net_income") / F.col("equity"))
    .withColumn("net_cash", F.col("cash") - F.col("total_debt"))
)
save(fundamentals, "company_fundamentals")

print("Gold transformation completed.")
