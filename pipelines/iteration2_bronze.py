import argparse

from common.paths import DATA_ROOT
from common.spark_session import get_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-dir",
        default="input_sample",
        help="folder under data/ to read trip/zone data from (default: input_sample)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = DATA_ROOT / args.data_dir

    spark = get_spark("iteration2_bronze")

    trips = spark.read.parquet(str(data_dir / "trips"))
    zones = spark.read.option("header", True).csv(str(data_dir / "taxi_zone_lookup.csv"))

    trips.printSchema()
    zones.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()
