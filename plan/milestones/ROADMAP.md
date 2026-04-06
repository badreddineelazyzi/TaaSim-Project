# 🗺️ TaaSim — Project Roadmap

> **Transport as a Service — Urban Mobility Platform**  
> Casablanca, Morocco · 8 Weeks · 2025–2026

---

## Timeline Overview

```
Week 1  ██████████  M1: Infrastructure Setup & Data Exploration
Week 2  ██████████  M2: Storage Design & Data Architecture
Week 3  ██████████  M3: Stream Processing I — GPS Pipeline
Week 4  ██████████  M4: Stream Processing II — Demand & Trip Matching
Week 5  ██████████  M5: Batch ETL + Spark Analytics
Week 6  ██████████  M6: ML Pipeline — Demand Forecasting
Week 7  ██████████  M7: Security + Integration Testing
Week 8  ██████████  M8: Demo Day + Investor Pitch
```

---

## Milestones

### 🟢 M1 — Infrastructure Setup & Data Exploration (Week 1)
> Foundation — Get the platform running and understand the data

| # | Issue | Priority | Estimate |
|---|---|---|---|
| 1 | [Provision Docker Compose Stack](milestone-1/issue-1-docker-compose.md) | 🔴 Critical | 3–4h |
| 2 | [Download & Upload Datasets to MinIO](milestone-1/issue-2-dataset-download.md) | 🟠 High | 1–2h |
| 3 | [Porto Dataset Profiling in Jupyter](milestone-1/issue-3-data-profiling.md) | 🟠 High | 2–3h |
| 4 | [Porto → Casablanca Zone Remapping](milestone-1/issue-4-zone-remapping.md) | 🟠 High | 2–3h |
| 5 | [Kafka Data Producers (Streaming Simulation)](milestone-1/issue-5-kafka-producers.md) | 🟠 High | 3–4h |

**Deliverables**: Docker stack running · Jupyter notebook · Zone-remapped map · Kafka events flowing

---

### 🟢 M2 — Storage Design & Data Architecture (Week 2)
> Storage — Design and deploy the persistence layer

| # | Issue | Priority | Estimate |
|---|---|---|---|
| 6 | [MinIO Bucket Structure & Kafka Connect S3 Sink](milestone-2/issue-6-minio-kafka-connect.md) | 🟠 High | 2–3h |
| 7 | [Cassandra Schema Design & Deployment](milestone-2/issue-7-cassandra-schema.md) | 🔴 Critical | 2–3h |
| 8 | [Architecture Decision Record (ADR)](milestone-2/issue-8-adr.md) | 🟡 Medium | 1–2h |

**Deliverables**: MinIO receiving data · Cassandra schema deployed · ADR submitted

---

### ⚡ M3 — Stream Processing I — GPS Pipeline (Week 3)
> Flink Job 1 — The foundational real-time pipeline

| # | Issue | Priority | Estimate |
|---|---|---|---|
| 9 | [Flink Job 1: GPS Normalizer](milestone-3/issue-9-flink-gps-normalizer.md) | 🔴 Critical | 4–5h |
| 10 | [Flink Checkpointing Configuration](milestone-3/issue-10-flink-checkpointing.md) | 🟠 High | 1–2h |
| 11 | [Grafana Live Vehicle Position Map](milestone-3/issue-11-grafana-vehicle-map.md) | 🟠 High | 1–2h |
| 12 | [Watermark & Late Event Testing](milestone-3/issue-12-watermark-testing.md) | 🟠 High | 1–2h |

**Deliverables**: Flink Job 1 running · Grafana live map · Watermark tested

---

### 🗂️ M4 — Stream Processing II — Demand & Trip Matching (Week 4)
> Flink Jobs 2 & 3 — Complete the real-time engine

| # | Issue | Priority | Estimate |
|---|---|---|---|
| 13 | [Flink Job 2: Demand Aggregator](milestone-4/issue-13-flink-demand-aggregator.md) | 🔴 Critical | 3–4h |
| 14 | [Flink Job 3: Trip Matcher](milestone-4/issue-14-flink-trip-matcher.md) | 🔴 Critical | 4–5h |
| 15 | [Grafana Demand Heatmap Panel](milestone-4/issue-15-grafana-heatmap.md) | 🟠 High | 1–2h |

**Deliverables**: End-to-end trip flow < 5s · Demand heatmap updating · RocksDB state

---

### 🗂️ M5 — Batch ETL + Spark Analytics (Week 5)
> Spark — Offline processing and business intelligence

| # | Issue | Priority | Estimate |
|---|---|---|---|
| 16 | [Spark ETL: Porto Dataset Cleaning & Enrichment](milestone-5/issue-16-spark-etl-porto.md) | 🔴 Critical | 3–4h |
| 17 | [Spark ETL: NYC TLC Demand Aggregation](milestone-5/issue-17-spark-etl-nyc.md) | 🟠 High | 2–3h |
| 18 | [Spark SQL KPI Computation & Grafana Panel](milestone-5/issue-18-kpi-grafana.md) | 🟠 High | 2–3h |

**Deliverables**: Porto ETL < 5 min · NYC ETL done · Grafana KPI panel

---

### 🤖 M6 — ML Pipeline — Demand Forecasting (Week 6)
> Machine Learning — From reactive to proactive

| # | Issue | Priority | Estimate |
|---|---|---|---|
| 19 | [Spark Feature Engineering for Demand Forecasting](milestone-6/issue-19-feature-engineering.md) | 🔴 Critical | 3–4h |
| 20 | [GBT Model Training & Evaluation](milestone-6/issue-20-model-training.md) | 🔴 Critical | 3–4h |
| 21 | [FastAPI Demand Forecast Endpoint](milestone-6/issue-21-fastapi-forecast.md) | 🟠 High | 2–3h |
| 22 | [Grafana ML Forecast Overlay Panel](milestone-6/issue-22-grafana-ml-overlay.md) | 🟡 Medium | 1–2h |

**Deliverables**: Model beats baseline · API < 500ms · Grafana forecast overlay

---

### 🔐 M7 — Security + Integration Testing (Week 7)
> Hardening — Lock it down and prove it works

| # | Issue | Priority | Estimate |
|---|---|---|---|
| 23 | [JWT Authentication on FastAPI](milestone-7/issue-23-jwt-auth.md) | 🔴 Critical | 2–3h |
| 24 | [Full Integration Test & SLA Measurement](milestone-7/issue-24-integration-test.md) | 🔴 Critical | 3–4h |
| 25 | [HTTPS & Kafka Topic ACLs](milestone-7/issue-25-https-acls.md) | 🟡 Medium | 1–2h |

**Deliverables**: JWT working · SLA table · Checkpoint recovery recorded

---

### 🎤 M8 — Demo Day + Investor Pitch (Week 8)
> Showtime — Polish, rehearse, deliver

| # | Issue | Priority | Estimate |
|---|---|---|---|
| 26 | [Polish Grafana Dashboard for Demo](milestone-8/issue-26-grafana-polish.md) | 🔴 Critical | 2–3h |
| 27 | [Live Demo Script & Anomaly Injection Rehearsal](milestone-8/issue-27-demo-script.md) | 🔴 Critical | 2–3h |
| 28 | [10-Slide Pitch Deck](milestone-8/issue-28-pitch-deck.md) | 🟠 High | 2–3h |
| 29 | [Technical Report (12–15 Pages)](milestone-8/issue-29-technical-report.md) | 🔴 Critical | 4–6h |

**Deliverables**: Live demo · Pitch deck · Technical report

---

## Summary Statistics

| Metric | Value |
|---|---|
| **Total Milestones** | 8 |
| **Total Issues** | 29 |
| **Critical Priority** | 14 |
| **High Priority** | 12 |
| **Medium Priority** | 3 |
| **Total Estimated Hours** | ~70–90 hours |
| **Duration** | 8 weeks |

---

## Critical Path

The following issues are on the **critical path** — delays here directly impact the Demo Day:

```
M1 #1 Docker → M2 #7 Cassandra → M3 #9 Flink Job 1 → M4 #14 Trip Matcher → M5 #16 Spark ETL → M6 #20 ML Training → M7 #24 Integration Test → M8 #27 Demo
```

## Evaluation Weights

| Pillar | Weight | Key Milestones |
|---|---|---|
| Pipeline Completeness | 30% | M1, M3, M4, M5 |
| Engineering Quality | 25% | M2, M3, M7 |
| Technical Report | 25% | M8 (Issue #29) |
| Startup Pitch + Demo | 20% | M8 (Issues #27, #28) |

---

> *"The best time to build the data infrastructure for Moroccan mobility was 10 years ago. The second best time is now."*
