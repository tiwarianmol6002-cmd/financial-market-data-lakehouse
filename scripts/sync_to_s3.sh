#!/usr/bin/env bash
# Uploads local raw parquet files to S3. Usage: BUCKET=... ./scripts/sync_to_s3.sh
set -euo pipefail
BUCKET="${BUCKET:-anmol-financial-lakehouse-2026}"

for folder in stock_prices company_metadata financials; do
  aws s3 sync "data/raw/$folder/" "s3://$BUCKET/raw/$folder/"
done
aws s3 ls "s3://$BUCKET/raw/" --recursive --human-readable | head -50
