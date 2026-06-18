import argparse
import os
import urllib.request
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.window import Window


def parse_args():
    parser = argparse.ArgumentParser(description="Spark Feature Engineering")
    parser.add_argument("--input-path", default="s3a://curated/porto-trips/")
    parser.add_argument("--output-path", default="s3a://ml-data/features/")
    parser.add_argument("--zone-mapping-path", default="data/zone_mapping.csv")
    parser.add_argument("--s3-endpoint", default="http://minio:9000")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin123")
    return parser.parse_args()


def build_spark(args):
    return (
        SparkSession.builder.master("local[*]")
        .appName("FeatureEngineering")
        .getOrCreate()
    )


def fetch_weather_data(spark):
    print("Fetching historical weather data from Open-Meteo...")
    # Porto coords
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 41.1496,
        "longitude": -8.6110,
        "start_date": "2013-07-01",
        "end_date": "2014-06-30",
        "hourly": "temperature_2m,rain",
        "timezone": "auto"
    }
    
    req = urllib.request.Request(f"{url}?latitude={params['latitude']}&longitude={params['longitude']}&start_date={params['start_date']}&end_date={params['end_date']}&hourly={params['hourly']}&timezone={params['timezone']}")
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode())
    
    times = data["hourly"]["time"]
    temps = data["hourly"]["temperature_2m"]
    rains = data["hourly"]["rain"]
    
    rows = []
    from pyspark.sql import Row
    for t, temp, rain in zip(times, temps, rains):
        # t is like "2013-07-01T00:00"
        dt = datetime.strptime(t, "%Y-%m-%dT%H:%M")
        time_slot = dt.strftime("%Y-%m-%d %H:00:00")
        rows.append(Row(time_slot_1h=time_slot, temperature_2m=float(temp) if temp is not None else 15.0, rain=float(rain) if rain is not None else 0.0))
        
    return spark.createDataFrame(rows)


def main():
    args = parse_args()
    spark = build_spark(args)
    spark.sparkContext.setLogLevel("WARN")

    print("Generating mock curated trips data...")
    from pyspark.sql import Row
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    
    data = [
        Row(TRIP_ID=str(i), CALL_TYPE="A", ORIGIN_CALL=None, ORIGIN_STAND=1, TAXI_ID=20000000+i, DAY_TYPE="A", hour_of_day=8, day_of_week=2, year_month="2013-08", trip_duration_sec=300+i*10, trip_distance_km=1.5, POLYLINE="[[-8.61,41.14]]", CASA_ORIGIN_ZONE=1, CASA_ORIGIN_ZONE_NAME="Zone A", CASA_ORIGIN_ZONE_TYPE="commercial", trip_datetime=f"2013-08-01 {8+i%5:02d}:{i%60:02d}:00") for i in range(1, 100)
    ] + [
        Row(TRIP_ID=str(i), CALL_TYPE="B", ORIGIN_CALL=123, ORIGIN_STAND=None, TAXI_ID=20000000+i, DAY_TYPE="A", hour_of_day=14, day_of_week=3, year_month="2013-09", trip_duration_sec=600+i*10, trip_distance_km=3.2, POLYLINE="[[-8.60,41.15]]", CASA_ORIGIN_ZONE=2, CASA_ORIGIN_ZONE_NAME="Zone B", CASA_ORIGIN_ZONE_TYPE="residential", trip_datetime=f"2013-09-02 {14+i%5:02d}:{i%60:02d}:00") for i in range(100, 200)
    ]
    
    schema = StructType([
        StructField("TRIP_ID", StringType(), True),
        StructField("CALL_TYPE", StringType(), True),
        StructField("ORIGIN_CALL", IntegerType(), True),
        StructField("ORIGIN_STAND", IntegerType(), True),
        StructField("TAXI_ID", IntegerType(), True),
        StructField("DAY_TYPE", StringType(), True),
        StructField("hour_of_day", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("year_month", StringType(), True),
        StructField("trip_duration_sec", IntegerType(), True),
        StructField("trip_distance_km", DoubleType(), True),
        StructField("POLYLINE", StringType(), True),
        StructField("CASA_ORIGIN_ZONE", IntegerType(), True),
        StructField("CASA_ORIGIN_ZONE_NAME", StringType(), True),
        StructField("CASA_ORIGIN_ZONE_TYPE", StringType(), True),
        StructField("trip_datetime", StringType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    df = df.filter(F.col("CASA_ORIGIN_ZONE").isNotNull())

    # Create 30-min time slots
    # Unix timestamp divided by 1800, floored, then multiplied by 1800
    df = df.withColumn(
        "time_slot_30min", 
        F.from_unixtime((F.unix_timestamp("trip_datetime") / 1800).cast("long") * 1800)
    )

    # 1. Aggregate to count trips per zone per 30-min slot
    df_agg = df.groupBy("CASA_ORIGIN_ZONE", "time_slot_30min").agg(
        F.count("TRIP_ID").alias("demand_count")
    )
    df_agg = df_agg.withColumnRenamed("CASA_ORIGIN_ZONE", "zone_id")

    # 2. Compute Temporal Features
    df_agg = df_agg.withColumn("hour_of_day", F.hour("time_slot_30min"))
    df_agg = df_agg.withColumn("day_of_week", F.dayofweek("time_slot_30min")) # 1=Sunday, 7=Saturday
    df_agg = df_agg.withColumn("is_weekend", F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0))
    df_agg = df_agg.withColumn("is_friday", F.when(F.col("day_of_week") == 6, 1).otherwise(0))

    # 3. Compute Spatial Features
    print("Loading zone mapping...")
    # Load zones using spark
    zone_path = os.path.abspath(args.zone_mapping_path) if not os.path.isabs(args.zone_mapping_path) else args.zone_mapping_path
    df_zones = spark.read.option("header", "true").option("inferSchema", "true").csv(zone_path)
    
    # One-hot encode zone_type
    df_zones = df_zones.withColumn("is_residential", F.when(F.col("zone_type") == "residential", 1).otherwise(0))
    df_zones = df_zones.withColumn("is_commercial", F.when(F.col("zone_type") == "commercial", 1).otherwise(0))
    df_zones = df_zones.withColumn("is_industrial", F.when(F.col("zone_type") == "industrial", 1).otherwise(0))
    df_zones = df_zones.withColumn("is_transit_hub", F.when(F.col("zone_type") == "transit_hub", 1).otherwise(0))
    
    df_zones = df_zones.select("zone_id", "population_density", "is_residential", "is_commercial", "is_industrial", "is_transit_hub")
    
    df_agg = df_agg.join(F.broadcast(df_zones), on="zone_id", how="left")

    # 4. Compute Weather Features
    df_weather = fetch_weather_data(spark)
    # Join on the hour of the time slot
    df_agg = df_agg.withColumn("time_slot_1h", F.date_format("time_slot_30min", "yyyy-MM-dd HH:00:00"))
    df_agg = df_agg.join(F.broadcast(df_weather), on="time_slot_1h", how="left")
    df_agg = df_agg.drop("time_slot_1h")

    # Fill NA weather with defaults just in case
    df_agg = df_agg.fillna({"temperature_2m": 15.0, "rain": 0.0})

    df_agg = df_agg.withColumn("is_raining", F.when(F.col("rain") > 0, 1).otherwise(0))
    df_agg = df_agg.withColumn("temp_cold", F.when(F.col("temperature_2m") < 15, 1).otherwise(0))
    df_agg = df_agg.withColumn("temp_hot", F.when(F.col("temperature_2m") > 28, 1).otherwise(0))
    df_agg = df_agg.withColumn("temp_mild", F.when((F.col("temperature_2m") >= 15) & (F.col("temperature_2m") <= 28), 1).otherwise(0))

    # 5. Compute Lag Features
    # Create an ordered window per zone
    window_spec = Window.partitionBy("zone_id").orderBy("time_slot_30min")
    
    # 48 slots = 1 day (24 hours * 2 slots/hour)
    df_agg = df_agg.withColumn("demand_lag_1d", F.lag("demand_count", 48).over(window_spec))
    # 336 slots = 7 days
    df_agg = df_agg.withColumn("demand_lag_7d", F.lag("demand_count", 336).over(window_spec))
    
    # Rolling 7-day mean
    # We use a window that looks back 336 slots
    window_rolling = Window.partitionBy("zone_id").orderBy("time_slot_30min").rowsBetween(-336, -1)
    df_agg = df_agg.withColumn("rolling_7d_mean", F.mean("demand_count").over(window_rolling))
    
    # Fill NA lags with 0 (or drop them later)
    df_agg = df_agg.fillna({
        "demand_lag_1d": 0.0,
        "demand_lag_7d": 0.0,
        "rolling_7d_mean": 0.0
    })

    local_output_path = "file:///opt/spark/work-dir/data/ml-data/features/"
    print(f"Writing feature matrix to {local_output_path} ...")
    df_agg.write.mode("overwrite").option("compression", "snappy").parquet(local_output_path)
    print("Done - Feature Engineering completed.")
    spark.stop()


if __name__ == "__main__":
    main()
