import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from common.spark_session import get_spark

DATA_DIR = REPO_ROOT / "data"
SAMPLE_DIR = REPO_ROOT / "sample_data"
SOURCE_MONTH = "2026-01"
SAMPLE_ROWS = 5_000
SEED = 42


def main():
    spark = get_spark("make_sample_data")

    trips = spark.read.parquet(str(DATA_DIR / "trips" / f"yellow_tripdata_{SOURCE_MONTH}.parquet"))
    total_rows = trips.count()

    trips_sample = trips.sample(withReplacement=False, fraction=SAMPLE_ROWS / total_rows, seed=SEED)
    trips_sample.coalesce(1).write.mode("overwrite").parquet(str(SAMPLE_DIR / "trips"))

    shutil.copy(DATA_DIR / "taxi_zone_lookup.csv", SAMPLE_DIR / "taxi_zone_lookup.csv")

    written_count = spark.read.parquet(str(SAMPLE_DIR / "trips")).count()
    print(f"Wrote {written_count} sample trip rows to {SAMPLE_DIR / 'trips'}")

    spark.stop()


if __name__ == "__main__":
    main()
