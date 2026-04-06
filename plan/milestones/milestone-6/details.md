# 🤖 Milestone 6 — ML Pipeline — Demand Forecasting

## Overview

**Duration**: Week 6  
**Theme**: Machine Learning — Transform TaaSim from reactive to proactive  
**Goal**: Build the complete ML pipeline: feature engineering, GBT model training, evaluation against naive baseline, model artifact storage, and FastAPI serving endpoint. Overlay ML forecasts on the Grafana dashboard.

## Objectives

- [ ] Feature engineering Spark job (temporal, spatial, weather, lag features)
- [ ] GBTRegressor trained on 10-month Porto data
- [ ] Model evaluated: RMSE must beat naive 7-day-lag baseline
- [ ] Model artifact saved to MinIO `ml/models/demand_v1/`
- [ ] FastAPI endpoint `POST /api/demand/forecast` serving predictions
- [ ] Grafana ML forecast overlay on demand heatmap

## Acceptance Criteria

✅ Model RMSE beats naive 7-day-lag baseline  
✅ Feature importance chart with top 3 predictors explained  
✅ API `/demand/forecast` responds in < 500ms  
✅ Grafana shows ML forecast vs actual demand  

## Dependencies

- Milestone 5 (Curated trip data in MinIO)

## Labels

`ml` `spark` `forecasting` `api` `week-6`
