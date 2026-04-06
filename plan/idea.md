# TaaSim — Transport as a Service · Urban Mobility Platform

> *Casablanca, Morocco · 2025–2026*

## 🎯 Vision

**"Build the data platform that moves Casablanca."**

TaaSim is a **Transport-as-a-Service** platform that treats urban mobility as a **data engineering problem**. By ingesting GPS vehicle streams, processing citizen trip reservations in real time, and applying batch analytics and machine learning to historical patterns, TaaSim can:

- **Match riders to vehicles dynamically**
- **Forecast demand surges**
- **Give city planners a unified analytical view** of the mobility network

## 🏙️ Problem Statement

Casablanca is home to **4+ million inhabitants**. Despite significant investment in transit infrastructure (BRT, ONCF rail, taxis), urban mobility remains deeply fragmented:

| Pain Point | Reality |
|---|---|
| **No shared data layer** | Grand taxis, petits taxis, and informal minibuses operate with no GPS tracking, no digital booking |
| **Demand blindness** | Drivers cruise for passengers; passengers wait with no visibility |
| **No interoperability** | Formal transit and informal taxis share no data, no ticketing |
| **Cash-only payments** | Zero payment data = zero analytics, zero personalization |
| **Underserved periphery** | New districts grow faster than routes are planned |

## 📊 Datasets

### Primary: Porto Taxi Trajectories (ECML/PKDD 2015)
- **~1.7M taxi trips** · ~1.5 GB compressed CSV
- **442 taxis** · July 2013 – June 2014
- **Key fields**: TRIP_ID, CALL_TYPE, TAXI_ID, TIMESTAMP, POLYLINE (GPS every 15s)
- Porto zones are **remapped to Casablanca's 16 arrondissements**

### Secondary: NYC TLC Trip Records
- **~10M rows/month** · Parquet format
- Used for **batch processing only** — Spark ETL, KPI computation, ML training
- Students use 3 months (~30M rows)

### Real-Time Simulation Layer
- `vehicle_gps_producer.py` — replays Porto GPS at 10× speed with noise
- `trip_request_producer.py` — generates reservations following demand curves
- `event_injector.py` — injects anomalies (demand spikes, GPS blackouts, rain)

## 🏗️ Architecture — Kappa

| Layer | Technology | Role |
|---|---|---|
| **Messaging** | Apache Kafka (KRaft) | Central event bus: GPS pings + trip reservations |
| **Object Store** | MinIO (S3-compatible) | Data lake: raw, curated, ML zones |
| **Batch + ML** | Apache Spark (PySpark) | ETL, feature engineering, GBT demand forecasting |
| **Streaming** | Apache Flink | Real-time GPS normalization, demand aggregation, trip matching |
| **Database** | Apache Cassandra | Low-latency serving: positions, trips, demand zones |
| **Dashboard** | Grafana | Live vehicle map, demand heatmap, KPI panels |
| **API** | FastAPI (Python) | REST: trip reservation, vehicle lookup, demand forecast |

### Flink Processing Jobs
1. **Job 1 — GPS Normalizer**: Validate, deduplicate, watermark, zone-map GPS events
2. **Job 2 — Demand Aggregator**: 30s tumbling window per zone, supply/demand ratio
3. **Job 3 — Trip Matcher**: Match riders to nearest vehicles, compute ETA

### Cassandra Tables
- `vehicle_positions` — partition: `(city, zone_id)`, clustering: `event_time DESC`
- `trips` — partition: `(city, date_bucket)`, clustering: `created_at DESC`
- `demand_zones` — partition: `(city, zone_id)`, clustering: `window_start DESC`

## 🤖 ML Pipeline — Demand Forecasting

- **Target**: Number of trip requests per zone per 30-min slot
- **Algorithm**: GBTRegressor (Spark MLlib)
- **Features**: temporal, spatial, weather, lag demand
- **Baseline**: Naive 7-day-lag prediction (must beat this)
- **Serving**: FastAPI endpoint `POST /api/demand/forecast`

## 📏 Key Performance Targets

| Metric | Target |
|---|---|
| Trip match latency | < 5 seconds P95 |
| Vehicle position freshness | < 15 seconds |
| Demand zone update frequency | Every 30 seconds |
| ML forecast API response | < 500ms at 20 req/s |
| Spark ETL (1.7M rows) | < 5 minutes |

## 🔐 Security Requirements

- **JWT authentication** on all FastAPI endpoints (rider + admin roles)
- **GPS anonymization** — snap to zone centroid before persisting
- **Kafka Topic ACLs** — producers write to `raw.*` only
- **HTTPS** on API (self-signed cert for demo)

## 📅 Timeline

**8 weeks** · Weekly lab sessions (~3 hours each) · Team of 2–3 students

## 📊 Evaluation

| Pillar | Weight |
|---|---|
| Pipeline Completeness | 30% |
| Engineering Quality | 25% |
| Technical Report | 25% |
| Startup Pitch + Demo | 20% |

## 🏆 Extension Challenges

- Schema evolution with Avro (+3%)
- Driver earnings tracker (+3%)
- Dynamic pricing (+4%)
- Kappa vs Lambda comparison (+4%)
- Real Porto coordinates view (+2%)
