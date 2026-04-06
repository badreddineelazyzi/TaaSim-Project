# Issue #10 — Flink Checkpointing Configuration

**Milestone**: 3 — Stream Processing I — GPS Pipeline  
**Labels**: `streaming` `flink` `reliability` `priority-high`  
**Assignees**: TBD  
**Estimate**: 1–2 hours

## Description

Configure Flink checkpointing to MinIO for all streaming jobs. Checkpointing guarantees at-least-once processing — no GPS events are lost on failure.

## Requirements

- [ ] Enable Flink checkpointing every **60 seconds**
- [ ] Configure checkpoint storage to MinIO (S3A path)
- [ ] Set state backend to **RocksDB** for scalable keyed state
- [ ] Configure checkpoint timeout and minimum pause between checkpoints
- [ ] Verify checkpoint files appear in MinIO after job runs

## Configuration

```yaml
# flink-conf.yaml additions
state.backend: rocksdb
state.checkpoints.dir: s3a://flink-checkpoints/
state.savepoints.dir: s3a://flink-savepoints/
execution.checkpointing.interval: 60000
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.timeout: 120000
execution.checkpointing.min-pause: 30000
```

## Verification Test

- [ ] Start Flink Job 1 with GPS producer running
- [ ] Wait for at least 2 checkpoints to complete
- [ ] Verify checkpoint files in MinIO
- [ ] Kill Flink Task Manager manually
- [ ] Restart Task Manager → verify job recovers from last checkpoint
- [ ] Confirm no data loss / duplication (check Cassandra row counts)

## Acceptance Criteria

- [ ] Checkpointing configured and running every 60 seconds
- [ ] Checkpoint files stored in MinIO
- [ ] Job recovers from manual Task Manager restart
- [ ] Recovery documented with evidence (screenshots/logs)
