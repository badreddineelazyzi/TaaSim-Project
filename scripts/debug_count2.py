from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DebugCount2")\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")\
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")\
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw = spark.read.option("header", "true").option("inferSchema", "true").csv("s3a://raw/porto-trips/")
print(f"Raw: {raw.count()} rows, {raw.select('TRIP_ID').distinct().count()} distinct")

import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, DoubleType

polyline_schema = ArrayType(ArrayType(DoubleType()))
d = raw.withColumn("coords", F.from_json(F.col("POLYLINE"), polyline_schema))
d = d.filter((F.col("MISSING_DATA") == False) & F.col("coords").isNotNull() & (F.size("coords") > 0))
d = d.dropDuplicates(["TRIP_ID"])
print(f"Cleaned: {d.count()} rows, {d.select('TRIP_ID').distinct().count()} distinct")

d2 = d.select("*", F.posexplode("coords").alias("pos", "coord"))
d2 = d2.withColumn("porto_lon", F.col("coord")[0])
d2 = d2.withColumn("porto_lat", F.col("coord")[1])
print(f"Exploded: {d2.count()} rows, {d2.select('TRIP_ID').distinct().count()} distinct")

# Check the trip_distance
from pyspark.sql.window import Window
w = Window.partitionBy("TRIP_ID").orderBy("pos")
d2 = d2.withColumn("prev_porto_lon", F.lag("porto_lon").over(w))
d2 = d2.withColumn("prev_porto_lat", F.lag("porto_lat").over(w))
EARTH_RADIUS_KM = 6371.0
lat1_r = F.radians(F.col("prev_porto_lat"))
lat2_r = F.radians(F.col("porto_lat"))
lon1_r = F.radians(F.col("prev_porto_lon"))
lon2_r = F.radians(F.col("porto_lon"))
dlat = lat2_r - lat1_r
dlon = lon2_r - lon1_r
a = F.pow(F.sin(dlat / 2), 2) + F.cos(lat1_r) * F.cos(lat2_r) * F.pow(F.sin(dlon / 2), 2)
seg_dist = F.lit(2 * EARTH_RADIUS_KM) * F.asin(F.sqrt(a))
seg_dist = F.when(F.col("prev_porto_lon").isNotNull(), seg_dist).otherwise(F.lit(0.0))
d2 = d2.withColumn("segment_km", seg_dist)
d2 = d2.withColumn("trip_distance_km", F.sum("segment_km").over(Window.partitionBy("TRIP_ID")))
td = d2.select("TRIP_ID", "trip_distance_km").dropDuplicates(["TRIP_ID"])
print(f"Trip distance table: {td.count()} distinct")

# Check casa_polylines
cp = d2.groupBy("TRIP_ID").agg(F.collect_list(F.struct(F.col("pos"), F.col("porto_lon"), F.col("porto_lat"))).alias("pts"))
print(f"Casa polylines: {cp.count()} distinct")

# Check temporal
temporal = d.select("TRIP_ID", F.from_unixtime(F.col("TIMESTAMP")).cast("timestamp").alias("dt"))
print(f"Temporal: {temporal.count()} distinct")
print(f"Null datetimes: {temporal.filter(F.col('dt').isNull()).count()}")

spark.stop()
