import argparse
from pyspark.sql import SparkSession

def parse_args():
    parser = argparse.ArgumentParser(description="Verify Feature Engineering output")
    parser.add_argument("--input-path", default="s3a://ml-data/features/")
    parser.add_argument("--s3-endpoint", default="http://minio:9000")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin123")
    return parser.parse_args()

def build_spark(args):
    return (
        SparkSession.builder.appName("VerifyFeatures")
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

    df = spark.read.parquet(args.input_path)
    print(f"Total rows in feature matrix: {df.count()}")
    
    print("\nSchema:")
    df.printSchema()
    
    print("\nSample Rows:")
    df.show(5, truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    main()
