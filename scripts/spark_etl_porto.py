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

    df = df.filter(F.col("MISSING_DATA") == False)
    df = df.limit(5000)
    df = df.dropDuplicates(["TRIP_ID"])

    # Remove parsing of coords to avoid memory/serialization explosion
    # df = df.withColumn("coords", F.from_json(F.col("POLYLINE"), polyline_schema))
    # df = df.filter(F.col("coords").isNotNull() & (F.size("coords") > 0))

    df = df.withColumn("trip_datetime", F.from_unixtime(F.col("TIMESTAMP")).cast(TimestampType()))
    df = df.withColumn("hour_of_day", F.hour("trip_datetime"))
    df = df.withColumn("day_of_week", F.dayofweek("trip_datetime"))
    df = df.withColumn("year_month", F.date_format("trip_datetime", "yyyy-MM"))

    # duration will be calculated in UDF

    import math
    import json
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    out_schema = StructType([
        StructField("trip_distance_km", DoubleType(), True),
        StructField("new_polyline", StringType(), True),
        StructField("CASA_ORIGIN_ZONE", IntegerType(), True),
        StructField("trip_duration_sec", IntegerType(), True)
    ])

    @F.udf(returnType=out_schema)
    def process_trip_udf(polyline_str):
        if not polyline_str or polyline_str == '[]':
            return None
            
        try:
            coords = json.loads(polyline_str)
        except:
            return None
            
        if not coords:
            return None
            
        casa_pts = []
        trip_distance_km = 0.0
        prev_porto_lon = None
        prev_porto_lat = None
        
        for coord in coords:
            if not coord or len(coord) < 2:
                continue
            porto_lon, porto_lat = coord[0], coord[1]
            
            c_lon = max(PORTO_LON_MIN, min(porto_lon, PORTO_LON_MAX))
            c_lat = max(PORTO_LAT_MIN, min(porto_lat, PORTO_LAT_MAX))
            
            c_casa_lon = casa_lon_min + ((c_lon - PORTO_LON_MIN) / (PORTO_LON_MAX - PORTO_LON_MIN)) * (casa_lon_max - casa_lon_min)
            c_casa_lat = casa_lat_min + ((c_lat - PORTO_LAT_MIN) / (PORTO_LAT_MAX - PORTO_LAT_MIN)) * (casa_lat_max - casa_lat_min)
            
            casa_pts.append([c_casa_lon, c_casa_lat])
            
            if prev_porto_lon is not None and prev_porto_lat is not None:
                lat1_r = math.radians(prev_porto_lat)
                lat2_r = math.radians(porto_lat)
                lon1_r = math.radians(prev_porto_lon)
                lon2_r = math.radians(porto_lon)
                dlat = lat2_r - lat1_r
                dlon = lon2_r - lon1_r
                a = math.sin(dlat/2)**2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon/2)**2
                a = max(0.0, min(1.0, a))
                seg_dist = 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))
                trip_distance_km += seg_dist
                
            prev_porto_lon = porto_lon
            prev_porto_lat = porto_lat
            
        if not casa_pts:
            return None
            
        start_lon, start_lat = casa_pts[0][0], casa_pts[0][1]
        best_zone = None
        min_dist = float('inf')
        for z in zones:
            d = (start_lat - z["lat"])**2 + (start_lon - z["lon"])**2
            if d < min_dist:
                min_dist = d
                best_zone = z["zone_id"]
                
        return {
            "trip_distance_km": float(trip_distance_km),
            "new_polyline": json.dumps(casa_pts),
            "CASA_ORIGIN_ZONE": best_zone,
            "trip_duration_sec": len(coords) * 15
        }

    df = df.withColumn("processed", process_trip_udf(F.col("POLYLINE")))
    df = df.filter(F.col("processed").isNotNull())
    df = df.withColumn("trip_distance_km", F.col("processed.trip_distance_km"))
    df = df.withColumn("POLYLINE", F.col("processed.new_polyline"))
    df = df.withColumn("CASA_ORIGIN_ZONE", F.col("processed.CASA_ORIGIN_ZONE"))
    df = df.withColumn("trip_duration_sec", F.col("processed.trip_duration_sec"))

    zone_meta = spark.createDataFrame(
        [(z["zone_id"], z["zone_name"], z["zone_type"]) for z in zones],
        ["zone_id", "zone_name", "zone_type"]
    )
    df = df.join(zone_meta, df.CASA_ORIGIN_ZONE == zone_meta.zone_id, "left")
    df = df.drop("zone_id").withColumnRenamed("zone_name", "CASA_ORIGIN_ZONE_NAME").withColumnRenamed("zone_type", "CASA_ORIGIN_ZONE_TYPE")

    df_out = df.drop("processed", "trip_datetime")

    print(f"Writing {args.output_path} ...")
    df_out.write.mode("overwrite").option("compression", "snappy").partitionBy("year_month").parquet(args.output_path)
    print("Done - Porto ETL completed.")
    spark.stop()


if __name__ == "__main__":
    main()
