from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("InspectNYC")\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")\
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")\
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("s3a://raw/nyc-tlc/")
print(f"Row count: {df.count()}")
print("\nSchema:")
df.printSchema()
print("\nSample rows:")
df.show(3, truncate=False)

spark.stop()
