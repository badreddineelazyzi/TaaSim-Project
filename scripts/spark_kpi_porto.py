from pyspark.sql import SparkSession, functions as F
import os

spark = SparkSession.builder.appName("PortoKPIComputation")\
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")\
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")\
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")\
    .config("spark.hadoop.fs.s3a.path.style.access", "true")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
    .config("spark.sql.shuffle.partitions", "8")\
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

INPUT_PATH = "s3a://curated/porto-trips/"
OUTPUT_PATH = "s3a://curated/kpi-porto/"

print(f"Reading curated Porto data from {INPUT_PATH}")
df = spark.read.parquet(INPUT_PATH)
df = df.drop("POLYLINE", "start_lon", "start_lat")
df.cache()
total = df.count()
print(f"Total curated rows: {total}")

OUTPUT_CQL = "/opt/spark/scripts/kpi_inserts.cql"
cql_lines = []

def write_cql(kpi_name, rows):
    for r in rows:
        category = str(r[0]) if r[0] is not None else "null"
        subcategory = str(r[1]) if len(r) > 1 and r[1] is not None else ""
        label = str(r[2]) if len(r) > 2 and r[2] is not None else category
        value = float(r[-1])
        cat_esc = category.replace("'", "''")
        sub_esc = subcategory.replace("'", "''")
        lbl_esc = label.replace("'", "''")
        cql_lines.append(
            f"INSERT INTO taasim.kpi_aggregates (kpi_name, category, subcategory, label, value) "
            f"VALUES ('{kpi_name}', '{cat_esc}', '{sub_esc}', '{lbl_esc}', {value});"
        )

# ---------------------------------------------------------------------------
# KPI 1 — Trips per zone
# ---------------------------------------------------------------------------
print("\n[KPI 1] Trips per zone...")
kpi1 = df.groupBy("CASA_ORIGIN_ZONE", "CASA_ORIGIN_ZONE_NAME").agg(
    F.count("*").alias("trip_count")
).orderBy(F.desc("trip_count"))
kpi1.show(20, truncate=False)
kpi1.write.mode("overwrite").parquet(f"{OUTPUT_PATH}trips_per_zone/")
rows = kpi1.collect()
write_cql("trips_per_zone", [(r.CASA_ORIGIN_ZONE, "", r.CASA_ORIGIN_ZONE_NAME, r.trip_count) for r in rows])
print(f"  → {len(rows)} zones")

# ---------------------------------------------------------------------------
# KPI 2 — Avg trip duration per zone
# ---------------------------------------------------------------------------
print("\n[KPI 2] Avg trip duration per zone...")
kpi2 = df.groupBy("CASA_ORIGIN_ZONE", "CASA_ORIGIN_ZONE_NAME").agg(
    F.avg("trip_duration_sec").alias("avg_duration_sec")
).orderBy(F.desc("avg_duration_sec"))
kpi2.show(20, truncate=False)
kpi2.write.mode("overwrite").parquet(f"{OUTPUT_PATH}avg_duration_per_zone/")
rows = kpi2.collect()
write_cql("avg_duration_per_zone", [(r.CASA_ORIGIN_ZONE, "", r.CASA_ORIGIN_ZONE_NAME, r.avg_duration_sec) for r in rows])
print(f"  → {len(rows)} zones")

# ---------------------------------------------------------------------------
# KPI 3 — Peak demand hours
# ---------------------------------------------------------------------------
print("\n[KPI 3] Peak demand hours...")
kpi3 = df.groupBy("hour_of_day").agg(
    F.count("*").alias("trip_count")
).orderBy(F.desc("trip_count"))
kpi3.show(24, truncate=False)
kpi3.write.mode("overwrite").parquet(f"{OUTPUT_PATH}peak_demand_hours/")
rows = kpi3.collect()
write_cql("peak_demand_hours", [(f"{r.hour_of_day:02d}:00", "", f"Hour {r.hour_of_day}", r.trip_count) for r in rows])
print(f"  → {len(rows)} hours")

# ---------------------------------------------------------------------------
# KPI 4 — Coverage gap (zones with high demand but low service)
#   Metric: trips_per_vehicle_ratio = trip_count / 50 (assuming ~50 unique TAXI_IDs per zone is healthy)
#   Actually simpler: rank zones by trip count, flag bottom quartile as "underserved"
# ---------------------------------------------------------------------------
print("\n[KPI 4] Coverage gap analysis...")
zone_stats = df.groupBy("CASA_ORIGIN_ZONE", "CASA_ORIGIN_ZONE_NAME").agg(
    F.count("*").alias("trip_count"),
    F.countDistinct("TAXI_ID").alias("unique_taxis")
)
zone_stats = zone_stats.withColumn(
    "trips_per_taxi",
    F.round(F.col("trip_count") / F.when(F.col("unique_taxis") == 0, 1).otherwise(F.col("unique_taxis")), 1)
)
zone_stats = zone_stats.withColumn(
    "coverage_gap",
    F.when(F.col("trips_per_taxi") > 100, "underserved")
    .when(F.col("unique_taxis") < 5, "critical")
    .otherwise("adequate")
)
kpi4 = zone_stats.orderBy(F.desc("trips_per_taxi"))
kpi4.show(20, truncate=False)
kpi4.write.mode("overwrite").parquet(f"{OUTPUT_PATH}coverage_gap/")
rows = kpi4.collect()
write_cql("coverage_gap", [(r.CASA_ORIGIN_ZONE, r.coverage_gap, r.CASA_ORIGIN_ZONE_NAME, r.trips_per_taxi) for r in rows])
print(f"  → {len(rows)} zones analyzed")

# ---------------------------------------------------------------------------
# KPI 5 — Trip type distribution (CALL_TYPE: A=central, B=stand, C=street)
# ---------------------------------------------------------------------------
print("\n[KPI 5] Trip type distribution...")
kpi5 = df.groupBy("CALL_TYPE").agg(
    F.count("*").alias("trip_count")
).orderBy(F.desc("trip_count"))
label_map = {"A": "Central dispatch", "B": "Taxi stand", "C": "Street hail"}
kpi5 = kpi5.withColumn("type_label", F.create_map([F.lit(k) for kv in label_map.items() for k in kv])[F.col("CALL_TYPE")])
kpi5.show(10, truncate=False)
kpi5.write.mode("overwrite").parquet(f"{OUTPUT_PATH}trip_type_distribution/")
rows = kpi5.collect()
write_cql("trip_type_distribution", [(r.CALL_TYPE, "", r.type_label, r.trip_count) for r in rows])
print(f"  → {len(rows)} types")

# ---------------------------------------------------------------------------
# KPI 6 — Weekly trends (week-over-week trip volume)
# ---------------------------------------------------------------------------
print("\n[KPI 6] Weekly trends...")
kpi6 = df.withColumn("year_week", F.date_format(
    F.from_unixtime(F.col("TIMESTAMP").cast("int")), "yyyy-'W'ww"
))
kpi6 = kpi6.groupBy("year_week").agg(
    F.count("*").alias("trip_count")
).orderBy("year_week")
kpi6.show(30, truncate=False)
kpi6.write.mode("overwrite").parquet(f"{OUTPUT_PATH}weekly_trends/")
rows = kpi6.collect()
write_cql("weekly_trends", [(r.year_week, "", r.year_week, r.trip_count) for r in rows])
print(f"  → {len(rows)} weeks")

# ---------------------------------------------------------------------------
# Write CQL file
# ---------------------------------------------------------------------------
print(f"\nWriting CQL inserts to {OUTPUT_CQL}...")
with open(OUTPUT_CQL, "w") as f:
    f.write("USE taasim;\n")
    f.write("TRUNCATE kpi_aggregates;\n")
    for line in cql_lines:
        f.write(line + "\n")

print(f"CQL lines written: {len(cql_lines)}")

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print("\n========== KPI Summary ==========")
print(f"1. Trips per zone:          {len(rows)} zones (saved)")
# Re-read counts
for kpi_name in ["trips_per_zone", "avg_duration_per_zone", "peak_demand_hours", "coverage_gap", "trip_type_distribution", "weekly_trends"]:
    p = f"{OUTPUT_PATH}{kpi_name}"
    cnt = spark.read.parquet(p).count()
    print(f"   {kpi_name}: {cnt} rows → {p}")

print(f"\nCQL file: {OUTPUT_CQL}")
print("Done - Porto KPI computation completed.")

spark.stop()
