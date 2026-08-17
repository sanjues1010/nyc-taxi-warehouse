# nyc-taxi-warehouse

A local, end-to-end batch data platform built on PySpark, turning raw NYC TLC
Yellow Taxi trip records into a validated, queryable warehouse.

Built as a progression of iterations — beginner ingest/clean → medallion
lakehouse (bronze/silver/gold) → dimensional warehouse with full Airflow
orchestration. Each iteration is a standalone rebuild at increasing
sophistication, reusing the previous iteration's logic rather than reading
its persisted output as input.

## Data

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
  (monthly Yellow Taxi Parquet files) + the TLC Taxi Zone Lookup CSV.
- Real data is never committed to this repo — see
  `data_prep/download_data.sh`, which populates `data/input_main/`. A small
  token sample lives under `data/input_sample/` (committed) for running the
  pipeline end-to-end without the full dataset. Script-generated output goes
  to `data/output/<iteration>/<sample|input_main>/...` (gitignored).

## Layout

Pipeline code lives under `pipelines/`, one flat file per iteration/layer
(`iteration1_ingest_clean.py`, `iteration2_bronze.py`, ...) alongside a
shared `common/` package (`spark_session.py`, `paths.py`) — scripts import
it directly with no `sys.path` setup, since Python auto-adds a
directly-executed script's own directory to its import path.

`data_prep/` holds one-off data-acquisition utilities
(`download_data.sh`, `make_sample_data.py`) — run rarely, not part of the
pipeline itself, so they intentionally derive their own paths locally
rather than depending on `common/`.

See `KNOWN_LIMITATIONS.md` for concrete cases where one iteration's approach
breaks or falls short in practice, and which later iteration is expected to
close the gap.

## Setup

```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
