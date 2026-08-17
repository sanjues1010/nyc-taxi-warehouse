import argparse

from common.paths import INPUT_MAIN_DIR, INPUT_SAMPLE_DIR
from common.spark_session import get_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sample-data",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="read from data/input_sample/ (default) instead of data/input_main/",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = INPUT_SAMPLE_DIR if args.sample_data else INPUT_MAIN_DIR

    spark = get_spark("iteration2_bronze")

    trips = spark.read.parquet(str(data_dir / "trips"))
    zones = spark.read.option("header", True).csv(str(data_dir / "taxi_zone_lookup.csv"))

    trips.printSchema()
    zones.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()
