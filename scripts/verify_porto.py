from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("VerifyPorto")\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")\
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")\
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("s3a://curated/porto-trips/")
print(f"Row count: {df.count()}")

print("\nSchema:")
df.printSchema()

ym = df.select("year_month").distinct().orderBy("year_month").collect()
print(f"\nyear_month partitions ({len(ym)}):")
for r in ym:
    print(f"  {r.year_month}")

zones = df.select("CASA_ORIGIN_ZONE_NAME").distinct().orderBy("CASA_ORIGIN_ZONE_NAME").collect()
print(f"\nZones assigned ({len(zones)}):")
for r in zones:
    print(f"  {r.CASA_ORIGIN_ZONE_NAME}")

dedup_count = df.count() - df.select("TRIP_ID").distinct().count()
print(f"\nDup TRIP_IDs: {dedup_count}")

stats = df.select("trip_duration_sec", "trip_distance_km").summary("min", "max", "mean").collect()
print("\nTrip stats:")
for r in stats:
    print(f"  {r['summary']}: duration={r['trip_duration_sec']}, distance={r['trip_distance_km']}")

spark.stop()
