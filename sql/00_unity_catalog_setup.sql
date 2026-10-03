-- Run once in a Databricks SQL editor (Route A: AWS workspace + Unity Catalog).
-- Prerequisite: create a storage credential for the S3 bucket
--   Catalog Explorer -> External data -> Credentials (automated setup creates the IAM role),
--   or Catalog Explorer -> External Locations -> Create (quickstart).
-- Replace finance_s3_credential with your credential's name.

CREATE EXTERNAL LOCATION IF NOT EXISTS finance_raw
URL 's3://anmol-financial-lakehouse-2026/raw'
WITH (STORAGE CREDENTIAL finance_s3_credential);

SHOW EXTERNAL LOCATIONS;

CREATE CATALOG IF NOT EXISTS finance_lakehouse;
CREATE SCHEMA IF NOT EXISTS finance_lakehouse.bronze;
CREATE SCHEMA IF NOT EXISTS finance_lakehouse.silver;
CREATE SCHEMA IF NOT EXISTS finance_lakehouse.gold;

CREATE EXTERNAL VOLUME IF NOT EXISTS finance_lakehouse.bronze.raw_financial_data
LOCATION 's3://anmol-financial-lakehouse-2026/raw';

-- Check that files are visible:
LIST '/Volumes/finance_lakehouse/bronze/raw_financial_data/stock_prices/';

-- Route B (Databricks Free Edition, no custom S3): use a managed volume and upload the parquet files into it:
-- CREATE VOLUME IF NOT EXISTS finance_lakehouse.bronze.raw_financial_data;
