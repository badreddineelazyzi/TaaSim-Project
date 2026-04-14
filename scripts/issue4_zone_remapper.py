import json
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType, IntegerType

# Geographic Constants
PORTO_MIN_LAT = 41.13
PORTO_MIN_LON = -8.68
PORTO_LAT_HEIGHT = 41.19 - 41.13
PORTO_LON_WIDTH = -8.55 - (-8.68)

CASA_MIN_LAT = 33.48
CASA_MIN_LON = -7.68
CASA_LAT_HEIGHT = 33.63 - 33.48
CASA_LON_WIDTH = (-7.53) - (-7.68)

SCALE_LAT = CASA_LAT_HEIGHT / PORTO_LAT_HEIGHT
SCALE_LON = CASA_LON_WIDTH / PORTO_LON_WIDTH

# 16 Arrondissements
ZONES = [
    {"zone_id": 1, "lat": 33.593, "lon": -7.632},
    {"zone_id": 2, "lat": 33.585, "lon": -7.640},
    {"zone_id": 3, "lat": 33.592, "lon": -7.618},
    {"zone_id": 4, "lat": 33.570, "lon": -7.608},
    {"zone_id": 5, "lat": 33.575, "lon": -7.612},
    {"zone_id": 6, "lat": 33.606, "lon": -7.540},
    {"zone_id": 7, "lat": 33.596, "lon": -7.570},
    {"zone_id": 8, "lat": 33.600, "lon": -7.585},
    {"zone_id": 9, "lat": 33.560, "lon": -7.675},
    {"zone_id": 10, "lat": 33.542, "lon": -7.614},
    {"zone_id": 11, "lat": 33.590, "lon": -7.490},
    {"zone_id": 12, "lat": 33.575, "lon": -7.525},
    {"zone_id": 13, "lat": 33.555, "lon": -7.580},
    {"zone_id": 14, "lat": 33.550, "lon": -7.590},
    {"zone_id": 15, "lat": 33.540, "lon": -7.550},
    {"zone_id": 16, "lat": 33.545, "lon": -7.570},
]

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0 # Earth radius in kilometers
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return R * c

def transform_polyline(polyline_str):
    if not polyline_str or polyline_str == "[]":
        return "[]"
    try:
        points = json.loads(polyline_str)
        transformed_points = []
        for lon, lat in points:
            new_lat = CASA_MIN_LAT + (lat - PORTO_MIN_LAT) * SCALE_LAT
            new_lon = CASA_MIN_LON + (lon - PORTO_MIN_LON) * SCALE_LON
            transformed_points.append([new_lon, new_lat])
        return json.dumps(transformed_points)
    except Exception as e:
        return "[]"

def assign_zone(polyline_str):
    if not polyline_str or polyline_str == "[]":
        return None
    try:
        points = json.loads(polyline_str)
        if len(points) == 0:
            return None
        
        # Take the start point of the trip
        start_lon, start_lat = points[0]
        
        best_zone = None
        min_dist = float('inf')
        
        for z in ZONES:
            d = haversine(start_lat, start_lon, z["lat"], z["lon"])
            if d < min_dist:
                min_dist = d
                best_zone = z["zone_id"]
                
        return best_zone
    except Exception:
        return None

# UDF definitions
transform_polyline_udf = udf(transform_polyline, StringType())
assign_zone_udf = udf(assign_zone, IntegerType())

def main():
    spark = SparkSession.builder \
        .appName("PortoToCasablancaRemapper") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
        
    print("Spark Session created successfully.")
    
    # Read from MinIO
    # Note: AWS credentials are automatically picked up from spark-master env configuration
    input_path = "s3a://raw/porto-trips/"
    output_path = "s3a://curated/casablanca-trips-remapped/"
    
    print(f"Reading dataset from {input_path}")
    df = spark.read.option("header", "true").csv(input_path)
    
    print("Applying transformations...")
    # 1. Transform POLYLINE coordinates
    # 2. Extract ORIGIN_ZONE based on transformed coordinates
    df_transformed = df \
        .withColumn("REMAP_POLYLINE", transform_polyline_udf(col("POLYLINE"))) \
        .withColumn("CASA_ORIGIN_ZONE", assign_zone_udf(col("REMAP_POLYLINE")))
        
    # Drop old polyline and rename new
    df_final = df_transformed.drop("POLYLINE").withColumnRenamed("REMAP_POLYLINE", "POLYLINE")
    
    print(f"Writing dataset to {output_path}")
    # Write output to MinIO as parquet
    df_final.write.mode("overwrite").parquet(output_path)
    
    print("Done!")
    spark.stop()

if __name__ == "__main__":
    main()
