# nyc-taxi-warehouse

A local, end-to-end batch data platform built on PySpark, turning raw NYC TLC
Yellow Taxi trip records into a validated, queryable warehouse.

Built as a progression of iterations — beginner ingest/clean → medallion
lakehouse (bronze/silver/gold) → dimensional warehouse with full Airflow
orchestration. Each iteration is a standalone rebuild at increasing
sophistication, reusing the previous iteration's logic rather than reading
its persisted output as input.

## Data

- [NYC TLC Trip Record Data](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
  (monthly Yellow Taxi Parquet files) + the TLC Taxi Zone Lookup CSV.
- Real data is never committed to this repo — see `scripts/download_data.sh`.
  A small token sample lives under `sample_data/` for running the pipeline
  end-to-end without the full dataset.

## Setup

```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
