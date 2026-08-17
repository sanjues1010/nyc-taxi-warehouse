import shutil
import sys
from pathlib import Path

REPO_ROOT = next(p for p in Path(__file__).resolve().parents if (p / ".git").exists())
sys.path.insert(0, str(REPO_ROOT / "pipelines"))

from common.spark_session import get_spark

DATA_ROOT = REPO_ROOT / "data"
INPUT_SAMPLE_DIR = DATA_ROOT / "input_sample"
INPUT_MAIN_DIR = DATA_ROOT / "input_main"

SOURCE_MONTH = "2026-01"
SAMPLE_ROWS = 5_000
SEED = 42


def main():
    spark = get_spark("make_sample_data")

    trips = spark.read.parquet(str(INPUT_MAIN_DIR / "trips" / f"yellow_tripdata_{SOURCE_MONTH}.parquet"))
    total_rows = trips.count()

    trips_sample = trips.sample(withReplacement=False, fraction=SAMPLE_ROWS / total_rows, seed=SEED)
    trips_sample.coalesce(1).write.mode("overwrite").parquet(str(INPUT_SAMPLE_DIR / "trips"))

    shutil.copy(INPUT_MAIN_DIR / "taxi_zone_lookup.csv", INPUT_SAMPLE_DIR / "taxi_zone_lookup.csv")

    written_count = spark.read.parquet(str(INPUT_SAMPLE_DIR / "trips")).count()
    print(f"Wrote {written_count} sample trip rows to {INPUT_SAMPLE_DIR / 'trips'}")

    spark.stop()


if __name__ == "__main__":
    main()
