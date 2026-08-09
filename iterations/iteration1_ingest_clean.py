import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from pyspark.sql.functions import col, count, sum as spark_sum, when

from common.spark_session import get_spark

SAMPLE_DIR = REPO_ROOT / "sample_data"

REQUIRED_TRIP_FIELDS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
]


def clean_trips(trips):
    before = trips.count()

    trips = trips.dropna(subset=REQUIRED_TRIP_FIELDS)
    trips = trips.dropDuplicates()

    trips = (
        trips.withColumn("passenger_count", col("passenger_count").cast("int"))
        .withColumn("RatecodeID", col("RatecodeID").cast("int"))
        .withColumn("payment_type", col("payment_type").cast("int"))
    )

    after = trips.count()
    print(f"trips: {before} -> {after} rows after null handling + dedup")

    return trips


def assert_data_quality(trips):
    stats = trips.agg(
        count("*").alias("total"),
        spark_sum(when(col("fare_amount") < 0, 1).otherwise(0)).alias("negative_fares"),
        spark_sum(when(col("trip_distance") < 0, 1).otherwise(0)).alias("negative_distances"),
        spark_sum(
            when(col("tpep_dropoff_datetime") < col("tpep_pickup_datetime"), 1).otherwise(0)
        ).alias("bad_trip_times"),
    ).collect()[0]

    assert stats["total"] > 0, "trips is empty after cleaning"
    assert stats["negative_fares"] == 0, f"{stats['negative_fares']} rows have negative fare_amount"
    assert stats["negative_distances"] == 0, f"{stats['negative_distances']} rows have negative trip_distance"
    assert stats["bad_trip_times"] == 0, f"{stats['bad_trip_times']} rows have dropoff before pickup"

    print(
        f"DQ checks passed: {stats['total']} rows, "
        "no negative fare/distance, no dropoff-before-pickup"
    )


def busiest_pickup_zones(trips, zones):
    zones = zones.withColumn("LocationID", col("LocationID").cast("int"))

    return (
        trips.join(zones, trips["PULocationID"] == zones["LocationID"], "left")
        .groupBy("Zone", "Borough")
        .count()
        .orderBy(col("count").desc())
    )


def main():
    spark = get_spark("iteration1_ingest_clean")

    trips = spark.read.parquet(str(SAMPLE_DIR / "trips"))
    zones = spark.read.option("header", True).csv(str(SAMPLE_DIR / "taxi_zone_lookup.csv"))

    trips = clean_trips(trips)
    assert_data_quality(trips)
    trips.printSchema()
    zones.printSchema()

    busiest = busiest_pickup_zones(trips, zones)
    busiest.show(10, truncate=False)
    busiest.explain()

    spark.stop()


if __name__ == "__main__":
    main()
