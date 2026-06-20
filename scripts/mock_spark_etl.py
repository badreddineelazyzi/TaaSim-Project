import argparse
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-path", default="file:///opt/spark/work-dir/data/curated/porto-trips/")
    args, _ = parser.parse_known_args()

    spark = SparkSession.builder.appName("MockPortoETL").getOrCreate()

    data = [
        Row(TRIP_ID="1", CALL_TYPE="A", ORIGIN_CALL=None, ORIGIN_STAND=1, TAXI_ID=20000001, DAY_TYPE="A", hour_of_day=8, day_of_week=2, year_month="2013-08", trip_duration_sec=300, trip_distance_km=1.5, POLYLINE="[[-8.61,41.14]]", CASA_ORIGIN_ZONE=1, CASA_ORIGIN_ZONE_NAME="Zone A", CASA_ORIGIN_ZONE_TYPE="Commercial"),
        Row(TRIP_ID="2", CALL_TYPE="B", ORIGIN_CALL=123, ORIGIN_STAND=None, TAXI_ID=20000002, DAY_TYPE="A", hour_of_day=14, day_of_week=3, year_month="2013-09", trip_duration_sec=600, trip_distance_km=3.2, POLYLINE="[[-8.60,41.15]]", CASA_ORIGIN_ZONE=2, CASA_ORIGIN_ZONE_NAME="Zone B", CASA_ORIGIN_ZONE_TYPE="Residential")
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
        StructField("CASA_ORIGIN_ZONE_TYPE", StringType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    print(f"Writing mock data locally to {args.output_path}")
    df.write.mode("overwrite").option("compression", "snappy").partitionBy("year_month").parquet(args.output_path)
    print("Done")
    spark.stop()

if __name__ == "__main__":
    main()
