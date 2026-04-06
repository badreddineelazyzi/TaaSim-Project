# Issue #25 — HTTPS & Kafka Topic ACLs

**Milestone**: 7 — Security + Integration Testing  
**Labels**: `security` `kafka` `tls` `priority-medium`  
**Assignees**: TBD  
**Estimate**: 1–2 hours

## Description

Implement the remaining security requirements: HTTPS on the FastAPI service and Kafka topic ACLs.

## HTTPS on API

- [ ] Generate self-signed certificate (acceptable for demo)
- [ ] Configure TLS termination in uvicorn: `uvicorn --ssl-keyfile key.pem --ssl-certfile cert.pem`
- [ ] Verify API accessible via `https://`
- [ ] Document that this is demo-only (not production TLS)

## Kafka Topic ACLs

- [ ] Configure ACL rules:
  - Producers may only write to `raw.*` topics
  - Flink jobs read `raw.*` and write `processed.*`
  - Admin-only access to `processed.demand`
- [ ] Test ACL enforcement (attempt unauthorized write)

## Acceptance Criteria

- [ ] API responds over HTTPS
- [ ] Kafka ACLs configured and tested
- [ ] Security configuration documented
