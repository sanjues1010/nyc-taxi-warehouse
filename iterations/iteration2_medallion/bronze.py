import argparse
import sys
from pathlib import Path

REPO_ROOT = next(p for p in Path(__file__).resolve().parents if (p / ".git").exists())
sys.path.insert(0, str(REPO_ROOT))

from common.spark_session import get_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-dir",
        default="sample_data",
        help="folder under the repo root to read trip/zone data from (default: sample_data)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = REPO_ROOT / args.data_dir

    spark = get_spark("iteration2_bronze")

    trips = spark.read.parquet(str(data_dir / "trips"))
    zones = spark.read.option("header", True).csv(str(data_dir / "taxi_zone_lookup.csv"))

    trips.printSchema()
    zones.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()
