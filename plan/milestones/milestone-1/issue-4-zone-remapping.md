# Issue #4 — Porto → Casablanca Zone Remapping

**Milestone**: 1 — Infrastructure Setup & Data Exploration  
**Labels**: `data-engineering` `geospatial` `priority-high`  
**Assignees**: TBD  
**Estimate**: 2–3 hours

## Description

Implement a PySpark transformation that remaps Porto's 22 city zones to Casablanca's 16 arrondissements. This is a core data engineering task that makes the Porto proxy dataset feel authentic to the Casablanca context.

## Requirements

### Coordinate Transformation
- [ ] Define Porto bounding box (lat/lon bounds)
- [ ] Define Casablanca bounding box (lat/lon bounds)
- [ ] Implement linear transformation to map Porto GPS coordinates to Casablanca coordinates
- [ ] Apply transformation to all POLYLINE GPS points

### Zone Mapping
- [ ] Create `zone_mapping.csv` reference table mapping Porto zones to Casablanca arrondissements
- [ ] Include zone metadata: `zone_id`, `zone_name`, `zone_type` (residential/commercial/transit_hub), `population_density`, `centroid_lat`, `centroid_lon`
- [ ] Include adjacency list for each zone (needed later for trip matching fallback)

### PySpark Implementation
- [ ] Read Porto CSV from MinIO `raw/porto-trips/`
- [ ] Parse POLYLINE JSON column
- [ ] Apply coordinate transformation
- [ ] Assign zone IDs based on bounding box lookup
- [ ] Write remapped data back to MinIO

### Validation
- [ ] Visualize remapped trip origins on an OpenStreetMap Casablanca base map
- [ ] Verify trip-length distributions are preserved after transformation
- [ ] Spot-check at least 10 remapped trips visually

## Acceptance Criteria

- [ ] `zone_mapping.csv` created with all 16 arrondissements
- [ ] PySpark job runs successfully
- [ ] Visualization shows trips correctly placed within Casablanca geography
- [ ] Trip length distribution before/after comparison shows reasonable preservation
