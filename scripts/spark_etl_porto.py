import argparse
import csv
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, TimestampType
from pyspark.sql.window import Window

PORTO_LON_MIN, PORTO_LON_MAX = -8.690, -8.550
PORTO_LAT_MIN, PORTO_LAT_MAX = 41.100, 41.190
EARTH_RADIUS_KM = 6371.0


def parse_args():
    parser = argparse.ArgumentParser(description="Spark ETL: Porto Dataset Cleaning & Enrichment")
    parser.add_argument("--input-path", default="s3a://raw/porto-trips/")
    parser.add_argument("--output-path", default="s3a://curated/porto-trips/")
    parser.add_argument("--zone-mapping-path", default="data/zone_mapping.csv")
    parser.add_argument("--s3-endpoint", default="http://minio:9000")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin123")
    return parser.parse_args()


def load_zones(path):
    if not os.path.isabs(path):
        resolved = os.path.abspath(path)
    else:
        resolved = path
    if not os.path.exists(resolved):
        raise FileNotFoundError(f"zone mapping not found at '{resolved}'")
    zones = []
    with open(resolved, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            zones.append({
                "zone_id": int(row["zone_id"]),
                "zone_name": row.get("zone_name", ""),
                "zone_type": row.get("zone_type", ""),
                "lat": float(row["centroid_lat"]),
                "lon": float(row["centroid_lon"]),
            })
    return zones


def build_spark(args):
    return (
        SparkSession.builder.appName("PortoETL")
        .config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", args.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", args.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


def main():
    args = parse_args()
    spark = build_spark(args)
    spark.sparkContext.setLogLevel("WARN")

    print("Loading zone mapping...")
    zones = load_zones(args.zone_mapping_path)
    print(f"Loaded {len(zones)} zones")

    casa_lons = [z["lon"] for z in zones]
    casa_lats = [z["lat"] for z in zones]
    casa_lon_min, casa_lon_max = min(casa_lons), max(casa_lons)
    casa_lat_min = min(casa_lats) - 0.002
    casa_lat_max = max(casa_lats) + 0.002

    print(f"Reading raw Porto CSV from {args.input_path}...")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(args.input_path)

    polyline_schema = ArrayType(ArrayType(DoubleType()))
    df = df.withColumn("coords", F.from_json(F.col("POLYLINE"), polyline_schema))

    df = df.filter(
        (F.col("MISSING_DATA") == False) &
        F.col("coords").isNotNull() &
        (F.size("coords") > 0)
    )
    df = df.dropDuplicates(["TRIP_ID"])

    temporal = df.select(
        "TRIP_ID",
        F.from_unixtime(F.col("TIMESTAMP")).cast(TimestampType()).alias("trip_datetime")
    ).withColumn("hour_of_day", F.hour("trip_datetime")
    ).withColumn("day_of_week", F.dayofweek("trip_datetime")
    ).withColumn("year_month", F.date_format("trip_datetime", "yyyy-MM"))

    df = df.withColumn("trip_duration_sec", F.size("coords") * 15)

    df_exploded = df.select(
        "*", F.posexplode("coords").alias("pos", "coord")
    )
    df_exploded = df_exploded.withColumn("porto_lon", F.col("coord")[0])
    df_exploded = df_exploded.withColumn("porto_lat", F.col("coord")[1])

    porto_lon_clamped = (
        F.when(F.col("porto_lon") < F.lit(PORTO_LON_MIN), F.lit(PORTO_LON_MIN))
        .when(F.col("porto_lon") > F.lit(PORTO_LON_MAX), F.lit(PORTO_LON_MAX))
        .otherwise(F.col("porto_lon"))
    )
    porto_lat_clamped = (
        F.when(F.col("porto_lat") < F.lit(PORTO_LAT_MIN), F.lit(PORTO_LAT_MIN))
        .when(F.col("porto_lat") > F.lit(PORTO_LAT_MAX), F.lit(PORTO_LAT_MAX))
        .otherwise(F.col("porto_lat"))
    )

    df_exploded = df_exploded.withColumn(
        "casa_lon",
        F.lit(casa_lon_min) + ((porto_lon_clamped - F.lit(PORTO_LON_MIN))
                               / F.lit(PORTO_LON_MAX - PORTO_LON_MIN))
        * F.lit(casa_lon_max - casa_lon_min)
    ).withColumn(
        "casa_lat",
        F.lit(casa_lat_min) + ((porto_lat_clamped - F.lit(PORTO_LAT_MIN))
                               / F.lit(PORTO_LAT_MAX - PORTO_LAT_MIN))
        * F.lit(casa_lat_max - casa_lat_min)
    )

    w = Window.partitionBy("TRIP_ID").orderBy("pos")
    df_exploded = df_exploded.withColumn("prev_porto_lon", F.lag("porto_lon").over(w))
    df_exploded = df_exploded.withColumn("prev_porto_lat", F.lag("porto_lat").over(w))

    lat1_r = F.radians(F.col("prev_porto_lat"))
    lat2_r = F.radians(F.col("porto_lat"))
    lon1_r = F.radians(F.col("prev_porto_lon"))
    lon2_r = F.radians(F.col("porto_lon"))
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(lat1_r) * F.cos(lat2_r) * F.pow(F.sin(dlon / 2), 2)
    seg_dist = F.lit(2 * EARTH_RADIUS_KM) * F.asin(F.sqrt(a))
    seg_dist = F.when(F.col("prev_porto_lon").isNotNull(), seg_dist).otherwise(F.lit(0.0))
    df_exploded = df_exploded.withColumn("segment_km", seg_dist)

    df_exploded = df_exploded.withColumn(
        "trip_distance_km", F.sum("segment_km").over(Window.partitionBy("TRIP_ID"))
    )

    trip_distance = df_exploded.groupBy("TRIP_ID").agg(
        F.max("trip_distance_km").alias("trip_distance_km")
    )

    casa_polylines = df_exploded.groupBy("TRIP_ID").agg(
        F.collect_list(F.struct(F.col("pos"), F.col("casa_lon"), F.col("casa_lat"))).alias("pts")
    ).withColumn(
        "pts_sorted",
        F.expr("transform(array_sort(pts), x -> array(x.casa_lon, x.casa_lat))")
    ).withColumn("POLYLINE", F.to_json(F.col("pts_sorted")))

    df_start = df_exploded.filter(F.col("pos") == 0).select(
        "TRIP_ID",
        F.col("casa_lon").alias("start_lon"),
        F.col("casa_lat").alias("start_lat")
    )

    zone_candidates = F.array(*[
        F.struct(
            (F.pow(F.col("start_lat") - F.lit(z["lat"]), 2)
             + F.pow(F.col("start_lon") - F.lit(z["lon"]), 2)).alias("dist"),
            F.lit(z["zone_id"]).alias("zone_id"),
        )
        for z in zones
    ])

    df_zones = df_start.withColumn(
        "CASA_ORIGIN_ZONE",
        F.element_at(F.array_sort(zone_candidates), 1).getField("zone_id")
    )

    zone_meta = spark.createDataFrame(
        [(z["zone_id"], z["zone_name"], z["zone_type"]) for z in zones],
        ["zone_id", "zone_name", "zone_type"]
    )
    df_zones = df_zones.join(zone_meta, df_zones.CASA_ORIGIN_ZONE == zone_meta.zone_id, "left")
    df_zones = df_zones.drop("zone_id").withColumnRenamed("zone_name", "CASA_ORIGIN_ZONE_NAME").withColumnRenamed("zone_type", "CASA_ORIGIN_ZONE_TYPE")

    base_cols = [c for c in df.columns if c not in {"POLYLINE", "coords"}]
    df_out = df.select(*base_cols)
    df_out = df_out.join(temporal.select("TRIP_ID", "hour_of_day", "day_of_week", "year_month"), on="TRIP_ID", how="inner")
    df_out = df_out.join(trip_distance, on="TRIP_ID", how="inner")
    df_out = df_out.join(casa_polylines.select("TRIP_ID", "POLYLINE"), on="TRIP_ID", how="inner")
    df_out = df_out.join(df_zones, on="TRIP_ID", how="left")

    print(f"Writing {args.output_path} ...")
    df_out.write.mode("overwrite").option("compression", "snappy").partitionBy("year_month").parquet(args.output_path)
    print("Done - Porto ETL completed.")
    spark.stop()


if __name__ == "__main__":
    main()
