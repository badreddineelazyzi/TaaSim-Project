# Issue #8 — Architecture Decision Record (ADR)

**Milestone**: 2 — Storage Design & Data Architecture  
**Labels**: `documentation` `architecture` `priority-medium`  
**Assignees**: TBD  
**Estimate**: 1–2 hours

## Description

Write a 1-page Architecture Decision Record documenting the key architecture and storage decisions made for TaaSim. This ADR will be referenced in the final technical report and must be defensible during the Week 8 Q&A.

## ADR Structure

```markdown
# ADR-001: TaaSim Platform Architecture

## Status
Accepted

## Context
[Why these decisions needed to be made]

## Decisions

### 1. Kappa Architecture (not Lambda)
- Why single processing path via Kafka + Flink
- Tradeoffs vs Lambda (dual batch + stream paths)

### 2. Storage Layer Choices
- Why MinIO for data lake (S3-compatible, local deployment)
- Why Cassandra for serving (partition-key-driven queries, write-heavy workload)
- Why not PostgreSQL / MongoDB / etc.

### 3. Cassandra Partition Key Design
- vehicle_positions: (city, zone_id) — query-driven, not entity-driven
- trips: (city, date_bucket) — prevents unbounded partition growth
- demand_zones: (city, zone_id) — zone-centric heatmap queries

### 4. Separation of Concerns: Flink vs Spark
- Flink: real-time stream processing (GPS, demand, matching)
- Spark: offline batch ETL and ML training
- Why not use Spark Structured Streaming for everything

### 5. MinIO Bucket Structure
- raw/ curated/ ml/ separation
- Kafka archive for replay capability

## Consequences
[What follows from these decisions — both positive and negative]
```

## Tasks

- [ ] Draft ADR following the structure above
- [ ] Include concrete justification for each decision
- [ ] Review with team members
- [ ] Submit final version

## Acceptance Criteria

- [ ] ADR covers all 5 decision areas
- [ ] Each decision includes context, rationale, and consequences
- [ ] ADR is 1–2 pages (concise, not verbose)
- [ ] Team can defend every choice verbally
