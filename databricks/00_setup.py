# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup: catalog and schemas
# MAGIC Creates the Unity Catalog objects. The external location / external volume pointing at S3
# MAGIC must already exist (see `sql/00_unity_catalog_setup.sql`).

# COMMAND ----------

dbutils.widgets.text("catalog", "finance_lakehouse")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

print(f"Catalog and schemas ready: {CATALOG}.[bronze|silver|gold]")
