# Issue #1 — Provision Docker Compose Stack

**Milestone**: 1 — Infrastructure Setup & Data Exploration  
**Labels**: `infrastructure` `setup` `priority-critical`  
**Assignees**: All team members  
**Estimate**: 3–4 hours

## Description

Create a `docker-compose.yml` that provisions all required services for the TaaSim platform on a single workstation.

## Requirements

### Services to include:
- **Apache Kafka** — KRaft mode (no Zookeeper), 1 broker
- **MinIO** — S3-compatible object store, single node
- **Apache Cassandra** — Single node, CQL port exposed
- **Apache Flink** — 1 Job Manager + 1 Task Manager
- **Apache Spark** — Master + 1 Worker
- **Grafana** — With Cassandra datasource plugin pre-installed

### Configuration:
- Flink and Spark must have access to MinIO via S3A connector (`hadoop-aws` JAR + `s3a://` paths)
- Cassandra CQL port must be exposed to host
- Grafana on port 3000 with Cassandra plugin
- All services on the same Docker network
- Resource constraints suitable for 8 GB RAM workstation

## Acceptance Criteria

- [ ] `docker-compose up -d` starts all services without errors
- [ ] All services pass health checks:
  - `kafka-topics.sh --list` succeeds
  - `mc ls` shows MinIO accessible
  - `cqlsh` connects to Cassandra
  - `flink list` returns (even if empty)
  - `spark-shell` launches
  - Grafana UI accessible on `http://localhost:3000`

## Notes

- Use specific version tags for all images (avoid `latest`)
- Consider adding a health check script for quick validation
- Document any environment-specific configuration in README
