import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline


def parse_args():
    parser = argparse.ArgumentParser(description="Spark ML Training")
    parser.add_argument("--features-path", default="s3a://ml-data/features/")
    parser.add_argument("--model-output-path", default="s3a://ml-data/models/demand_v1/")
    parser.add_argument("--s3-endpoint", default="http://minio:9000")
    parser.add_argument("--s3-access-key", default="minioadmin")
    parser.add_argument("--s3-secret-key", default="minioadmin123")
    return parser.parse_args()


def build_spark(args):
    spark = SparkSession.builder \
        .appName("ML_DemandForecasting") \
        .config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", args.s3_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", args.s3_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    return spark


def main():
    args = parse_args()
    spark = build_spark(args)
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading features from {args.features_path}...")
    df = spark.read.parquet(args.features_path)
    print(f"Feature rows: {df.count()}, columns: {len(df.columns)}")

    year_months = sorted(df.select("year_month").distinct().rdd.flatMap(lambda x: x).collect())
    print(f"Available year_months: {year_months}")

    train_months = [ym for ym in year_months if ym <= "2013-09"]
    test_months = [ym for ym in year_months if ym >= "2013-10"]

    if not train_months or not test_months:
        print("WARNING: Using default temporal split.")
        train_df = df.filter(F.col("year_month") <= "2013-09")
        test_df = df.filter(F.col("year_month") >= "2013-10")
    else:
        train_df = df.filter(F.col("year_month").isin(train_months))
        test_df = df.filter(F.col("year_month").isin(test_months))

    train_count = train_df.count()
    test_count = test_df.count()
    print(f"Train samples: {train_count}, Test samples: {test_count}")

    feature_cols = [
        "hour_of_day", "day_of_week", "is_weekend", "is_friday",
        "population_density", "is_residential", "is_commercial", "is_industrial", "is_transit_hub",
        "temperature_2m", "rain", "is_raining", "temp_cold", "temp_hot", "temp_mild",
        "demand_lag_1d", "demand_lag_7d", "rolling_7d_mean", "zone_id"
    ]

    for col in feature_cols:
        train_df = train_df.withColumn(col, F.col(col).cast("double"))
        test_df = test_df.withColumn(col, F.col(col).cast("double"))

    print("Constructing MLlib Pipeline...")
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="rawFeatures", handleInvalid="skip")
    scaler = StandardScaler(inputCol="rawFeatures", outputCol="scaledFeatures", withStd=True, withMean=False)

    gbt = GBTRegressor(
        featuresCol="scaledFeatures",
        labelCol="demand_count",
        maxIter=50,
        seed=42
    )

    pipeline = Pipeline(stages=[assembler, scaler, gbt])

    print("Setting up Cross-Validation...")
    paramGrid = (ParamGridBuilder()
                 .addGrid(gbt.maxDepth, [5, 7])
                 .build())

    evaluator = RegressionEvaluator(
        labelCol="demand_count",
        predictionCol="prediction",
        metricName="rmse"
    )

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=paramGrid,
        evaluator=evaluator,
        numFolds=3,
        parallelism=2
    )

    print("Training model...")
    cvModel = cv.fit(train_df)

    bestModel = cvModel.bestModel

    print("Evaluating model on test set...")
    predictions = bestModel.transform(test_df)

    rmse = evaluator.evaluate(predictions)
    mae_evaluator = RegressionEvaluator(labelCol="demand_count", predictionCol="prediction", metricName="mae")
    mae = mae_evaluator.evaluate(predictions)

    print(f"Model RMSE: {rmse:.4f}")
    print(f"Model MAE: {mae:.4f}")

    baseline_evaluator = RegressionEvaluator(
        labelCol="demand_count",
        predictionCol="demand_lag_7d",
        metricName="rmse"
    )
    baseline_rmse = baseline_evaluator.evaluate(
        test_df.withColumn("demand_lag_7d", F.col("demand_lag_7d").cast("double"))
    )
    print(f"Baseline RMSE (7-day lag): {baseline_rmse:.4f}")

    if rmse < baseline_rmse:
        print("SUCCESS: Model outperformed the baseline!")
    else:
        print("WARNING: Model did NOT outperform the naive baseline.")

    print("\nPer-Zone Evaluation:")
    zone_metrics = predictions.groupBy("zone_id").agg(
        F.sqrt(F.mean(F.pow(F.col("demand_count") - F.col("prediction"), 2))).alias("model_rmse"),
        F.sqrt(F.mean(F.pow(F.col("demand_count") - F.col("demand_lag_7d"), 2))).alias("baseline_rmse")
    )
    zone_metrics.orderBy("zone_id").show(truncate=False)

    print("\nExtracting Feature Importances...")
    gbt_model = bestModel.stages[-1]
    importances = gbt_model.featureImportances

    importance_list = [(feature_cols[i], float(importances[i])) for i in range(len(feature_cols))]
    importance_list.sort(key=lambda x: x[1], reverse=True)

    print("Top 5 Predictors:")
    for i, (feat, imp) in enumerate(importance_list[:5]):
        print(f"{i+1}. {feat}: {imp:.4f}")

    print(f"\nSaving best model to {args.model_output_path}...")
    bestModel.write().overwrite().save(args.model_output_path)

    print("Done - ML Pipeline completed.")
    spark.stop()


if __name__ == "__main__":
    main()
