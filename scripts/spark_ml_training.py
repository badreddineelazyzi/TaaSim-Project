import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml import Pipeline

def build_spark():
    return (
        SparkSession.builder.master("local[*]")
        .appName("ML_DemandForecasting")
        .getOrCreate()
    )

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    input_path = "file:///opt/spark/work-dir/data/ml-data/features/"
    model_output_path = "file:///opt/spark/work-dir/data/ml/models/demand_v1/"

    print(f"Reading features from {input_path}...")
    df = spark.read.parquet(input_path)
    
    # Cast boolean features to integer so VectorAssembler can handle them
    # Some of them might already be integers in the mock data, so we ignore errors

    print("Splitting data temporally...")
    # Recreate year_month from time_slot_30min for the split
    df = df.withColumn("year_month", F.substring(F.col("time_slot_30min"), 1, 7))

    # Using 2013-08 for training and 2013-09 for testing based on our mock data
    train_df = df.filter(F.col("year_month") == "2013-08")
    test_df = df.filter(F.col("year_month") == "2013-09")
    
    print(f"Train samples: {train_df.count()}, Test samples: {test_df.count()}")

    # Define numeric features
    feature_cols = [
        "hour_of_day", "day_of_week", "is_weekend", "is_friday", 
        "population_density", "is_residential", "is_commercial", "is_industrial", "is_transit_hub",
        "temperature_2m", "rain", "is_raining", "temp_cold", "temp_hot", "temp_mild",
        "demand_lag_1d", "demand_lag_7d", "rolling_7d_mean", "zone_id"
    ]

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

    print("Evaluating model...")
    predictions = cvModel.transform(test_df)
    
    # Calculate RMSE and MAE
    rmse = evaluator.evaluate(predictions)
    mae_evaluator = RegressionEvaluator(labelCol="demand_count", predictionCol="prediction", metricName="mae")
    mae = mae_evaluator.evaluate(predictions)
    
    print(f"Model RMSE: {rmse:.4f}")
    print(f"Model MAE: {mae:.4f}")

    # Baseline: demand_lag_7d
    baseline_evaluator = RegressionEvaluator(
        labelCol="demand_count",
        predictionCol="demand_lag_7d",
        metricName="rmse"
    )
    baseline_rmse = baseline_evaluator.evaluate(test_df.withColumn("demand_lag_7d", F.col("demand_lag_7d").cast("double")))
    print(f"Baseline RMSE (7-day lag): {baseline_rmse:.4f}")
    
    if rmse < baseline_rmse:
        print("SUCCESS: Model outperformed the baseline!")
    else:
        print("WARNING: Model did NOT outperform the naive baseline.")

    print("\nPer-Zone Evaluation:")
    # Evaluate per zone
    zone_metrics = predictions.groupBy("CASA_ORIGIN_ZONE_NAME").agg(
        F.sqrt(F.mean(F.pow(F.col("demand_count") - F.col("prediction"), 2))).alias("model_rmse"),
        F.sqrt(F.mean(F.pow(F.col("demand_count") - F.col("demand_lag_7d"), 2))).alias("baseline_rmse")
    )
    zone_metrics.show(truncate=False)

    print("\nExtracting Feature Importances...")
    bestPipeline = cvModel.bestModel
    gbt_model = bestPipeline.stages[-1]
    importances = gbt_model.featureImportances
    
    importance_list = [(feature_cols[i], float(importances[i])) for i in range(len(feature_cols))]
    importance_list.sort(key=lambda x: x[1], reverse=True)
    
    print("Top Predictors:")
    for i, (feat, imp) in enumerate(importance_list[:5]):
        print(f"{i+1}. {feat}: {imp:.4f}")
        
    print(f"\nSaving model to {model_output_path}...")
    cvModel.write().overwrite().save(model_output_path)
    
    print("Done - ML Pipeline completed.")
    spark.stop()

if __name__ == "__main__":
    main()
