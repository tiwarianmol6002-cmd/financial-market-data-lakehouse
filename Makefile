.PHONY: setup test ingest upload bundle-validate deploy run

setup:
	python3 -m venv .venv && . .venv/bin/activate && pip install --upgrade pip && pip install -r requirements-dev.txt

test:
	pytest -q

ingest:
	python src/ingestion/market_data.py

upload:
	./scripts/sync_to_s3.sh

bundle-validate:
	databricks bundle validate -t dev

deploy:
	databricks bundle deploy -t dev

run:
	databricks bundle run financial_lakehouse_job -t dev
