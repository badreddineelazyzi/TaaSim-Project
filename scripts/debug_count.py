from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DebugCount")\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")\
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")\
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Count rows in curated output
df = spark.read.parquet("s3a://curated/porto-trips/")
print(f"Output row count: {df.count()}")
print(f"Distinct TRIP_IDs: {df.select('TRIP_ID').distinct().count()}")

# Also count raw data
raw = spark.read.option("header", "true").option("inferSchema", "true").csv("s3a://raw/porto-trips/")
print(f"Raw row count: {raw.count()}")
print(f"Raw distinct TRIP_IDs: {raw.select('TRIP_ID').distinct().count()}")

spark.stop()
