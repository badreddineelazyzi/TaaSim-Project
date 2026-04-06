# 🗂️ Milestone 2 — Storage Design & Data Architecture

## Overview

**Duration**: Week 2  
**Theme**: Storage — Design and deploy the persistence layer  
**Goal**: Finalize MinIO bucket structure, configure Kafka Connect S3 Sink for archival, design and deploy the Cassandra schema, and document all architectural decisions.

## Objectives

- [ ] MinIO bucket structure finalized: `raw/`, `curated/`, `ml/`, `kafka-archive/`
- [ ] Kafka Connect S3 Sink mirroring raw topics to MinIO
- [ ] Cassandra keyspace and tables deployed with justified partition keys
- [ ] Architecture Decision Record (ADR) written and submitted

## Acceptance Criteria

✅ MinIO buckets receiving data from Kafka Connect  
✅ Cassandra schema deployed and documented  
✅ ADR submitted (architecture and storage rationale)  

## Dependencies

- Milestone 1 completed (Docker stack running, Kafka producers emitting)

## Labels

`storage` `architecture` `cassandra` `minio` `week-2`
