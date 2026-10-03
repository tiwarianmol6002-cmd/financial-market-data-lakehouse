# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: raw files -> Delta (no business logic)
# MAGIC Keeps source columns as-is and adds ingestion metadata (`bronze_ingested_at`, `source_file`).

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "finance_lakehouse")
dbutils.widgets.text("raw_base", "/Volumes/finance_lakehouse/bronze/raw_financial_data")

CATALOG = dbutils.widgets.get("catalog")
RAW_BASE = dbutils.widgets.get("raw_base").rstrip("/")

# COMMAND ----------

def load_to_bronze(folder: str, table: str) -> None:
    df = (
        spark.read.format("parquet").load(f"{RAW_BASE}/{folder}/")
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("bronze_ingested_at", F.current_timestamp())
    )
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.bronze.{table}")
    )
    print(f"bronze.{table}: {spark.table(f'{CATALOG}.bronze.{table}').count()} rows")

# COMMAND ----------

load_to_bronze("stock_prices", "stock_prices")
load_to_bronze("company_metadata", "company_metadata")
load_to_bronze("financials", "financials")

print("Bronze ingestion completed.")
