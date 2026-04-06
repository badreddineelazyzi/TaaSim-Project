# Issue #20 — GBT Model Training & Evaluation

**Milestone**: 6 — ML Pipeline — Demand Forecasting  
**Labels**: `ml` `spark` `mllib` `priority-critical`  
**Assignees**: TBD  
**Estimate**: 3–4 hours

## Description

Train a Gradient Boosted Trees (GBT) regressor using Spark MLlib on the feature matrix to predict demand per zone per 30-minute slot. Evaluate against a naive 7-day-lag baseline.

## ML Pipeline Steps

### 1. Train/Test Split
- [ ] **Temporal split** — no random shuffle!
- [ ] Training: first 10 months of data
- [ ] Testing: last 2 months
- [ ] Filter on `year_month` column

### 2. Pipeline Construction
```python
assembler = VectorAssembler(inputCols=[...], outputCol="features")
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
gbt = GBTRegressor(
    featuresCol="scaledFeatures",
    labelCol="demand_count",
    maxDepth=5,
    maxIter=50
)
pipeline = Pipeline(stages=[assembler, scaler, gbt])
```

### 3. Cross-Validation
- [ ] `CrossValidator` with **3 folds**
- [ ] Tune only 2 parameters: `maxDepth` = [5, 7]
- [ ] Keep scope constrained (time constraint)

### 4. Evaluation
- [ ] Compute **RMSE** and **MAE** on test set
- [ ] Compute naive baseline: predict `demand_lag_7d` (same slot 7 days ago)
- [ ] Model passes **only if RMSE < baseline RMSE**
- [ ] Per-zone evaluation table: model vs baseline RMSE by zone

### 5. Feature Importance
- [ ] Extract feature importances from trained GBT model
- [ ] Generate feature importance chart
- [ ] Explain top 3 predictors in business context

### 6. Save Model
- [ ] `model.write().overwrite().save('s3a://ml/models/demand_v1/')`
- [ ] Log feature importances to a text file in MinIO

## Acceptance Criteria

- [ ] Model RMSE is lower than naive 7-day-lag baseline
- [ ] Evaluation table: model vs baseline per zone
- [ ] Feature importance chart generated
- [ ] Top 3 predictors explained in plain language
- [ ] Model artifact saved to MinIO `ml/models/demand_v1/`
