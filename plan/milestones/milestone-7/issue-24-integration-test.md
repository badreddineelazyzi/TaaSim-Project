# Issue #24 — Full Integration Test & SLA Measurement

**Milestone**: 7 — Security + Integration Testing  
**Labels**: `testing` `integration` `sla` `priority-critical`  
**Assignees**: All team members  
**Estimate**: 3–4 hours

## Description

Run a comprehensive 30-minute integration test with all system components running simultaneously. Measure all SLA targets defined in the project requirements.

## Integration Test Setup

- [ ] Start all 3 Flink jobs simultaneously
- [ ] Start GPS and trip request producers
- [ ] Start FastAPI service
- [ ] Run Spark ETL concurrently
- [ ] Monitor Grafana dashboard
- [ ] Run for **30 minutes** uninterrupted

## SLA Measurements

| Metric | Target | How to Measure | Result |
|---|---|---|---|
| Trip match latency | < 5s P95 | Compare Kafka request timestamp to Cassandra write in Job 3 | |
| Vehicle position freshness | < 15s | Compare Kafka producer timestamp to Cassandra write in Job 1 | |
| Demand zone update frequency | Every 30s | Verify `demand_zones` rows using `WRITETIME()` function | |
| ML forecast API response | < 500ms @ 20 req/s | Run `locust` load test | |
| Spark ETL (1.7M rows) | < 5 min | Check Spark UI job duration | |

## Checkpoint Recovery Test

- [ ] While all jobs are running, **manually kill Flink Task Manager**
- [ ] Restart Task Manager
- [ ] Verify all 3 Flink jobs recover from last checkpoint
- [ ] Confirm no data loss (compare expected vs actual Cassandra row counts)
- [ ] **Screen record** the entire recovery process

## Idempotent Write Verification

- [ ] Verify Cassandra writes use `IF NOT EXISTS` or upserts
- [ ] Confirm no duplicate trip records after Flink at-least-once redelivery
- [ ] Check `WRITETIME()` on problematic rows

## Deliverables

- [ ] SLA measurement table (filled in with actual results)
- [ ] Checkpoint recovery screen recording
- [ ] Integration test report (any failures, alerts, or issues noted)

## Acceptance Criteria

- [ ] All SLA targets met (or deviations documented with explanation)
- [ ] Checkpoint recovery demonstrated and recorded
- [ ] No duplicate records in Cassandra after recovery
- [ ] System stable for full 30-minute run
