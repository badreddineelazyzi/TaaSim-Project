# Issue #18 — Spark SQL KPI Computation & Grafana Panel

**Milestone**: 5 — Batch ETL + Spark Analytics  
**Labels**: `analytics` `spark` `grafana` `priority-high`  
**Assignees**: TBD  
**Estimate**: 2–3 hours

## Description

Use Spark SQL to compute weekly business KPIs from the curated trip data, load results to Cassandra, and visualize in a Grafana KPI panel.

## KPIs to Compute

| KPI | Description | SQL Approach |
|---|---|---|
| Trips per zone | Total trips by arrondissement | `GROUP BY zone_id` |
| Avg trip duration | Mean trip duration per zone | `AVG(duration_seconds)` |
| Peak demand hours | Hours with highest trip counts | `GROUP BY hour_of_day ORDER BY count DESC` |
| Coverage gap | Zones with demand but < 2 vehicles | `HAVING demand > threshold AND vehicles < 2` |
| Trip type distribution | Breakdown by CALL_TYPE (A/B/C) | `GROUP BY call_type` |
| Weekly trends | Week-over-week trip volume | `GROUP BY year_week` |

## Tasks

### Spark SQL
- [ ] Read curated Porto trips from `s3a://curated/porto-trips/`
- [ ] Compute all KPIs listed above
- [ ] Write aggregated KPIs to Cassandra `demand_zones` table (or new KPI table if needed)

### Grafana KPI Panel
- [ ] Create **Panel 3 — KPI Table** in Grafana dashboard
- [ ] Query Cassandra for trip data in last 24h
- [ ] Display:
  - Total trips
  - Average ETA
  - % matched within 5 seconds
  - Top 3 demand zones
- [ ] Format as stat panels or table

## Acceptance Criteria

- [ ] All 6 KPIs computed and available in Cassandra
- [ ] Grafana KPI panel shows business metrics
- [ ] KPIs refresh on dashboard reload
