import functools
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, coalesce, col, count, hour, lit, sum as spark_sum, when

from common.spark_session import get_spark

SAMPLE_DIR = REPO_ROOT / "sample_data"
CLEANED_DIR = SAMPLE_DIR / "cleaned"

REQUIRED_TRIP_FIELDS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
]

NEGATIVE_FARE_RATE_THRESHOLD = 0.05


def clean_trips(trips: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    before = trips.count()

    null_dropped = trips.filter(
        functools.reduce(lambda a, b: a | b, [col(c).isNull() for c in REQUIRED_TRIP_FIELDS])
    )

    trips_candidate = trips.dropna(subset=REQUIRED_TRIP_FIELDS)
    trips_candidate = trips_candidate.dropDuplicates()

    trips_candidate = (
        trips_candidate.withColumn("passenger_count", col("passenger_count").cast("int"))
        .withColumn("RatecodeID", col("RatecodeID").cast("int"))
        .withColumn("payment_type", col("payment_type").cast("int"))
    )

    negative_fares = trips_candidate.filter(col("fare_amount") < 0)
    trips_valid = trips_candidate.filter(col("fare_amount") >= 0)

    after = trips_valid.count()
    print(f"trips: {before} -> {after} rows after null handling + dedup + negative-fare split")
    print(f"null_dropped: {null_dropped.count()} rows excluded for missing required fields")
    print(f"negative_fares: {negative_fares.count()} rows excluded for negative fare_amount")

    return trips_valid, null_dropped, negative_fares


def _distance_and_time_stats(trips: DataFrame):
    return trips.agg(
        coalesce(
            spark_sum(when(col("trip_distance") < 0, 1).otherwise(0)), lit(0)
        ).alias("negative_distances"),
        coalesce(
            spark_sum(
                when(col("tpep_dropoff_datetime") < col("tpep_pickup_datetime"), 1).otherwise(0)
            ),
            lit(0),
        ).alias("bad_trip_times"),
    ).collect()[0]


def assert_data_quality(trips_valid: DataFrame, negative_fares: DataFrame) -> None:
    total = trips_valid.count()
    assert total > 0, "trips_valid is empty after cleaning"

    valid_stats = _distance_and_time_stats(trips_valid)
    assert valid_stats["negative_distances"] == 0, (
        f"{valid_stats['negative_distances']} trips_valid rows have negative trip_distance"
    )
    assert valid_stats["bad_trip_times"] == 0, (
        f"{valid_stats['bad_trip_times']} trips_valid rows have dropoff before pickup"
    )

    negative_fare_stats = _distance_and_time_stats(negative_fares)
    assert negative_fare_stats["negative_distances"] == 0, (
        f"{negative_fare_stats['negative_distances']} negative_fares rows have negative trip_distance"
    )
    assert negative_fare_stats["bad_trip_times"] == 0, (
        f"{negative_fare_stats['bad_trip_times']} negative_fares rows have dropoff before pickup"
    )

    negative_fare_count = negative_fares.count()
    negative_fare_rate = negative_fare_count / (total + negative_fare_count)
    assert negative_fare_rate <= NEGATIVE_FARE_RATE_THRESHOLD, (
        f"negative fare rate {negative_fare_rate:.2%} exceeds "
        f"{NEGATIVE_FARE_RATE_THRESHOLD:.0%} threshold"
    )

    print(
        f"DQ checks passed: {total} valid rows, "
        f"{negative_fare_count} negative-fare rows ({negative_fare_rate:.2%}, within "
        f"{NEGATIVE_FARE_RATE_THRESHOLD:.0%} threshold), no negative distance, "
        "no dropoff-before-pickup in either population"
    )


def busiest_pickup_zones(trips: DataFrame, zones: DataFrame) -> DataFrame:
    zones = zones.withColumn("LocationID", col("LocationID").cast("int"))

    return (
        trips.join(zones, trips["PULocationID"] == zones["LocationID"], "left")
        .groupBy("Zone", "Borough")
        .count()
        .orderBy(col("count").desc())
    )


def avg_fare_by_hour(trips: DataFrame) -> DataFrame:
    return (
        trips.groupBy(hour("tpep_pickup_datetime").alias("hour"))
        .agg(avg("fare_amount").alias("avg_fare"))
        .orderBy("hour")
    )


def tips_by_payment_type(trips: DataFrame) -> DataFrame:
    return (
        trips.groupBy("payment_type")
        .agg(avg("tip_amount").alias("avg_tip"), count("*").alias("trip_count"))
        .orderBy("payment_type")
    )


def write_outputs(
    trips_valid: DataFrame, null_dropped: DataFrame, negative_fares: DataFrame, run_dir: Path
) -> None:
    trips_valid.write.parquet(str(run_dir / "trips_valid"))
    null_dropped.write.parquet(str(run_dir / "null_dropped"))
    negative_fares.write.parquet(str(run_dir / "negative_fares"))
    print(f"wrote cleaned output to {run_dir}")


def main() -> None:
    spark = get_spark("iteration1_ingest_clean")

    trips = spark.read.parquet(str(SAMPLE_DIR / "trips"))
    zones = spark.read.option("header", True).csv(str(SAMPLE_DIR / "taxi_zone_lookup.csv"))

    trips_valid, null_dropped, negative_fares = clean_trips(trips)
    trips_valid.cache()
    negative_fares.cache()
    assert_data_quality(trips_valid, negative_fares)
    # trips_valid.printSchema()
    # zones.printSchema()

    busiest = busiest_pickup_zones(trips_valid, zones)
    busiest.show(10, truncate=False)
    # busiest.explain()

    avg_fare = avg_fare_by_hour(trips_valid)
    avg_fare.show(24, truncate=False)

    tips = tips_by_payment_type(trips_valid)
    tips.show(truncate=False)

    run_dir = CLEANED_DIR / datetime.now().strftime("%Y%m%d_%H%M%S")
    write_outputs(trips_valid, null_dropped, negative_fares, run_dir)

    trips_valid.unpersist()
    negative_fares.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
