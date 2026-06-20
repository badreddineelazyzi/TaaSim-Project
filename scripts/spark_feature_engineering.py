import argparse
import os
import urllib.request
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructField
from pyspark.sql.window import Window


CASA_LAT = 33.573
CASA_LON = -7.589


def parse_args():
    parser = argparse.ArgumentParser(description="Spark Feature Engineering")
    parser.add_argument("--input-path", default="s3a://curated/porto-trips/")
    parser.add_argument("--output-path", default="s3a://ml-data/features/")
    parser.add_argument("--zone-mapping-path", default="s3a://ml-data/zone_mapping.csv")
    parser.add_argument("--s3-endpoint", default="http://minio:9000")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin123")
    return parser.parse_args()


def build_spark(args):
    spark = SparkSession.builder \
        .appName("FeatureEngineering") \
        .config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", args.s3_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", args.s3_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    return spark


def fetch_weather_data(spark):
    print("Fetching historical weather data from Open-Meteo for Casablanca...")
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": CASA_LAT,
        "longitude": CASA_LON,
        "start_date": "2013-07-01",
        "end_date": "2014-06-30",
        "hourly": "temperature_2m,rain",
        "timezone": "auto"
    }
    full_url = (f"{url}?latitude={params['latitude']}&longitude={params['longitude']}"
                f"&start_date={params['start_date']}&end_date={params['end_date']}"
                f"&hourly={params['hourly']}&timezone={params['timezone']}")
    req = urllib.request.Request(full_url)
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode())

    times = data["hourly"]["time"]
    temps = data["hourly"]["temperature_2m"]
    rains = data["hourly"]["rain"]

    rows = []
    for t, temp, rain in zip(times, temps, rains):
        dt = datetime.strptime(t, "%Y-%m-%dT%H:%M")
        time_slot = dt.strftime("%Y-%m-%d %H:00:00")
        rows.append(Row(
            time_slot_1h=time_slot,
            temperature_2m=float(temp) if temp is not None else 15.0,
            rain=float(rain) if rain is not None else 0.0
        ))
    return spark.createDataFrame(rows)


def main():
    args = parse_args()
    spark = build_spark(args)
    spark.sparkContext.setLogLevel("WARN")

    # 0. Read curated Porto trips from S3
    print(f"Reading curated data from {args.input_path}...")
    df = spark.read.parquet(args.input_path)

    raw_count = df.count()
    print(f"Raw curated rows: {raw_count}")

    # Filter out rows without zone assignment
    df = df.filter(F.col("CASA_ORIGIN_ZONE").isNotNull())

    # Create proper datetime from TIMESTAMP (epoch seconds)
    df = df.withColumn("trip_datetime", F.from_unixtime(F.col("TIMESTAMP").cast("long")))

    # Create 30-min time slots
    df = df.withColumn(
        "time_slot_30min",
        F.from_unixtime(
            (F.unix_timestamp("trip_datetime") / 1800).cast("long") * 1800
        )
    )

    # 1. Aggregate: count trips per zone per 30-min slot
    df_agg = df.groupBy("CASA_ORIGIN_ZONE", "time_slot_30min").agg(
        F.count("TRIP_ID").alias("demand_count")
    )
    df_agg = df_agg.withColumnRenamed("CASA_ORIGIN_ZONE", "zone_id")
    print(f"Aggregated rows: {df_agg.count()}")

    # 2. Temporal Features
    df_agg = df_agg.withColumn("hour_of_day", F.hour("time_slot_30min"))
    df_agg = df_agg.withColumn("day_of_week", F.dayofweek("time_slot_30min"))
    df_agg = df_agg.withColumn("is_weekend",
                               F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0))
    df_agg = df_agg.withColumn("is_friday",
                               F.when(F.col("day_of_week") == 6, 1).otherwise(0))

    # 3. Spatial Features (zone mapping join)
    print("Loading zone mapping...")
    df_zones = spark.read.option("header", "true").option("inferSchema", "true").csv(args.zone_mapping_path)
    df_zones = df_zones.withColumnRenamed("zone_id", "zid")
    df_zones = df_zones.withColumn("is_residential",
                                   F.when(F.col("zone_type") == "residential", 1).otherwise(0))
    df_zones = df_zones.withColumn("is_commercial",
                                   F.when(F.col("zone_type") == "commercial", 1).otherwise(0))
    df_zones = df_zones.withColumn("is_industrial",
                                   F.when(F.col("zone_type") == "industrial", 1).otherwise(0))
    df_zones = df_zones.withColumn("is_transit_hub",
                                   F.when(F.col("zone_type") == "transit_hub", 1).otherwise(0))
    df_zones = df_zones.select("zid", "population_density",
                               "is_residential", "is_commercial",
                               "is_industrial", "is_transit_hub")
    df_agg = df_agg.join(F.broadcast(df_zones),
                         F.col("zone_id") == F.col("zid"), "left")
    df_agg = df_agg.drop("zid")

    # 4. Weather Features
    df_weather = fetch_weather_data(spark)
    df_agg = df_agg.withColumn("time_slot_1h",
                               F.date_format("time_slot_30min", "yyyy-MM-dd HH:00:00"))
    df_agg = df_agg.join(F.broadcast(df_weather), on="time_slot_1h", how="left")
    df_agg = df_agg.drop("time_slot_1h")
    df_agg = df_agg.fillna({"temperature_2m": 15.0, "rain": 0.0})
    df_agg = df_agg.withColumn("is_raining",
                               F.when(F.col("rain") > 0, 1).otherwise(0))
    df_agg = df_agg.withColumn("temp_cold",
                               F.when(F.col("temperature_2m") < 15, 1).otherwise(0))
    df_agg = df_agg.withColumn("temp_hot",
                               F.when(F.col("temperature_2m") > 28, 1).otherwise(0))
    df_agg = df_agg.withColumn("temp_mild",
                               F.when((F.col("temperature_2m") >= 15) &
                                      (F.col("temperature_2m") <= 28), 1).otherwise(0))

    # 5. Lag Features (ordered per zone)
    window_spec = Window.partitionBy("zone_id").orderBy("time_slot_30min")
    df_agg = df_agg.withColumn("demand_lag_1d",
                               F.lag("demand_count", 48).over(window_spec))
    df_agg = df_agg.withColumn("demand_lag_7d",
                               F.lag("demand_count", 336).over(window_spec))
    window_rolling = (Window.partitionBy("zone_id")
                      .orderBy("time_slot_30min")
                      .rowsBetween(-336, -1))
    df_agg = df_agg.withColumn("rolling_7d_mean",
                               F.mean("demand_count").over(window_rolling))
    df_agg = df_agg.fillna({
        "demand_lag_1d": 0.0,
        "demand_lag_7d": 0.0,
        "rolling_7d_mean": 0.0
    })

    # 6. Add year_month for temporal train/test split
    df_agg = df_agg.withColumn("year_month",
                               F.substring(F.col("time_slot_30min"), 1, 7))

    print(f"Final feature matrix: {df_agg.count()} rows, {len(df_agg.columns)} columns")
    print(f"Columns: {df_agg.columns}")

    # 7. Write to S3
    print(f"Writing feature matrix to {args.output_path} ...")
    df_agg.write.mode("overwrite").option("compression", "snappy").parquet(args.output_path)
    print("Done - Feature Engineering completed.")
    spark.stop()


if __name__ == "__main__":
    main()
