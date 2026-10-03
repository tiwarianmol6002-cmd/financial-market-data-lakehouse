#!/usr/bin/env bash
# Creates and locks down the S3 bucket. Usage: BUCKET=my-unique-name REGION=ap-south-1 ./scripts/setup_s3.sh
set -euo pipefail

BUCKET="${BUCKET:-anmol-financial-lakehouse-2026}"
REGION="${REGION:-ap-south-1}"

echo "Creating bucket s3://$BUCKET in $REGION ..."
if [ "$REGION" = "us-east-1" ]; then
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"
else
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
    --create-bucket-configuration LocationConstraint="$REGION"
fi

aws s3api put-public-access-block --bucket "$BUCKET" \
  --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

aws s3api put-bucket-versioning --bucket "$BUCKET" \
  --versioning-configuration Status=Enabled

aws s3api put-bucket-encryption --bucket "$BUCKET" \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

# Create the zone "folders"
for prefix in raw/stock_prices raw/company_metadata raw/financials processed/bronze processed/silver processed/gold archive; do
  aws s3api put-object --bucket "$BUCKET" --key "$prefix/" >/dev/null
done

echo "Done."; aws s3 ls "s3://$BUCKET/"
