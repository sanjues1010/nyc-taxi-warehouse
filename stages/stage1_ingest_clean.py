import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from common.spark_session import get_spark

SAMPLE_DIR = REPO_ROOT / "sample_data"


def main():
    spark = get_spark("stage1_ingest_clean")

    trips = spark.read.parquet(str(SAMPLE_DIR / "trips"))
    zones = spark.read.option("header", True).csv(str(SAMPLE_DIR / "taxi_zone_lookup.csv"))

    trips.printSchema()
    zones.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()
