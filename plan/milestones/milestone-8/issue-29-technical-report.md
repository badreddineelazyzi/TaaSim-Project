# Issue #29 — Technical Report (12–15 Pages)

**Milestone**: 8 — Demo Day + Investor Pitch  
**Labels**: `documentation` `report` `priority-critical`  
**Assignees**: All team members  
**Estimate**: 4–6 hours

## Description

Write the final technical report covering all aspects of the TaaSim platform. This document accounts for 25% of the final grade.

## Report Structure

### 1. Introduction (1 page)
- Problem statement
- TaaSim solution overview
- Report structure

### 2. Architecture (2 pages)
- Kappa architecture choice and justification
- Architecture diagram
- Technology stack with rationale
- Flink vs Spark separation of concerns

### 3. Dataset & Zone Remapping (1–2 pages)
- Porto dataset description
- NYC TLC dataset description
- Casablanca zone remapping methodology
- Visualization of remapped data

### 4. Data Model (1–2 pages)
- Cassandra schema design
- Partition key justification with query patterns
- MinIO bucket structure

### 5. Stream Processing (2 pages)
- Flink Job 1: GPS Normalizer
- Flink Job 2: Demand Aggregator
- Flink Job 3: Trip Matcher
- Watermark strategy with late event evidence

### 6. Batch Processing (1 page)
- Spark ETL pipeline
- KPI computation

### 7. ML Pipeline (2 pages)
- Feature engineering
- GBT training and evaluation
- **Model vs baseline RMSE comparison table** (per zone)
- **Feature importance chart** with top 3 predictors explained in business terms
- Serving via FastAPI

### 8. Security (1 page)
- JWT authentication
- GPS anonymization
- Kafka ACLs
- HTTPS

### 9. NFR Measurement (1 page)
- **SLA measurement table** (all targets from §6.1)
- Checkpoint recovery evidence

### 10. Post-Mortem & Lessons Learned (1 page)
- What worked well
- What failed and why
- What you would do differently
- Honest reflection (graded positively)

## Quality Checklist

- [ ] 12–15 pages total
- [ ] Architecture diagram included
- [ ] ML evaluation table: model vs baseline per zone
- [ ] Feature importance chart with business explanation
- [ ] SLA measurement table with actual results
- [ ] ADR included or referenced
- [ ] Honest post-mortem (not whitewashed)
- [ ] References cited

## Acceptance Criteria

- [ ] Report is 12–15 pages
- [ ] All 10 sections complete
- [ ] ML evaluation proves model beats baseline
- [ ] Team can defend every claim in Q&A
