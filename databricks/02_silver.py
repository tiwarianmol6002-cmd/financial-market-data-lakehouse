# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: cleaned, typed, de-duplicated

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "finance_lakehouse")
CATALOG = dbutils.widgets.get("catalog")

def save(df, table):
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOG}.silver.{table}"))
    print(f"silver.{table}: {spark.table(f'{CATALOG}.silver.{table}').count()} rows")

# COMMAND ----------
# MAGIC %md ## Stock prices

# COMMAND ----------

bronze_prices = spark.table(f"{CATALOG}.bronze.stock_prices")

silver_prices = (
    bronze_prices
    .withColumn("ticker", F.upper(F.trim("ticker")))
    .withColumn("trade_date", F.to_date("trade_date"))
    .withColumn("open", F.col("open").cast("double"))
    .withColumn("high", F.col("high").cast("double"))
    .withColumn("low", F.col("low").cast("double"))
    .withColumn("close", F.col("close").cast("double"))
    .withColumn("adjusted_close", F.col("adjusted_close").cast("double"))
    .withColumn("volume", F.col("volume").cast("long"))
    .withColumn("dividends", F.coalesce(F.col("dividends").cast("double"), F.lit(0.0)))
    .withColumn("stock_splits", F.coalesce(F.col("stock_splits").cast("double"), F.lit(0.0)))
    # invalid rows
    .filter(F.col("ticker").isNotNull() & F.col("trade_date").isNotNull())
    .filter(F.col("close").isNotNull() & (F.col("close") > 0))
    .filter(F.col("adjusted_close").isNotNull() & (F.col("adjusted_close") > 0))
    .filter(F.col("open") > 0)
    .filter(F.col("high") >= F.col("low"))
    .filter(F.col("volume").isNotNull() & (F.col("volume") >= 0))
    # keep the most recently ingested row per ticker/date
    .withColumn(
        "_rn",
        F.row_number().over(
            Window.partitionBy("ticker", "trade_date")
            .orderBy(F.col("ingested_at").desc())
        ),
    )
    .filter("_rn = 1")
    .drop("_rn")
)
save(silver_prices, "stock_prices")

# COMMAND ----------
# MAGIC %md ## Company metadata

# COMMAND ----------

metadata = (
    spark.table(f"{CATALOG}.bronze.company_metadata")
    .filter(F.col("ticker").isNotNull())
    .withColumn("ticker", F.upper(F.trim("ticker")))
    .withColumn("market_cap", F.col("market_cap").cast("double"))
    .withColumn("sector", F.coalesce("sector", F.lit("Unknown")))
    .dropDuplicates(["ticker"])
)
save(metadata, "company_metadata")

# COMMAND ----------
# MAGIC %md ## Financial statements (long format)

# COMMAND ----------

financials = (
    spark.table(f"{CATALOG}.bronze.financials")
    .withColumn("ticker", F.upper(F.trim("ticker")))
    .withColumn("report_date", F.to_date("report_date"))
    .withColumn("value", F.col("value").cast("double"))
    .filter(F.col("ticker").isNotNull() & F.col("report_date").isNotNull()
            & F.col("line_item").isNotNull() & F.col("value").isNotNull())
    .dropDuplicates(["ticker", "report_date", "statement_type", "line_item"])
)
save(financials, "financials")

print("Silver transformation completed.")
