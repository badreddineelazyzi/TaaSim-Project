from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--shuffle-partitions", type=int, default=8)
args = parser.parse_args()

spark = SparkSession.builder.appName("NYCTLCDemandAggregation")\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")\
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")\
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
    .config("spark.sql.shuffle.partitions", args.shuffle_partitions)\
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

INPUT_PATH = "s3a://raw/nyc-tlc/"
OUTPUT_PATH = "s3a://curated/demand-by-zone/"

print(f"Reading NYC TLC data from {INPUT_PATH}")
df = spark.read.parquet(INPUT_PATH)
total_rows = df.count()
print(f"Total raw rows: {total_rows}")

print("Computing demand aggregates per zone and hour...")
demand = df.groupBy(
    "PULocationID",
    F.hour("tpep_pickup_datetime").alias("hour_of_day"),
    F.date_format("tpep_pickup_datetime", "yyyy-MM").alias("month")
).agg(
    F.count("*").alias("trip_count"),
    F.avg("fare_amount").alias("avg_fare"),
    F.avg("trip_distance").alias("avg_distance"),
    F.avg("passenger_count").alias("avg_passenger_count")
).orderBy("PULocationID", "hour_of_day")

demand.cache()
print(f"Aggregated rows: {demand.count()}")
print("\nSample aggregates:")
demand.show(10, truncate=False)

print(f"\nWriting to {OUTPUT_PATH}")
demand.write.mode("overwrite").partitionBy("month").parquet(OUTPUT_PATH)

print(f"Verifying output...")
verify = spark.read.parquet(OUTPUT_PATH)
print(f"Output rows: {verify.count()}")
print(f"Months: {verify.select('month').distinct().orderBy('month').rdd.flatMap(lambda x: x).collect()}")

spark.stop()
print("Done - NYC demand aggregation completed.")
