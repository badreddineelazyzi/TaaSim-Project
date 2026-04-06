# Issue #12 — Watermark & Late Event Testing

**Milestone**: 3 — Stream Processing I — GPS Pipeline  
**Labels**: `testing` `flink` `streaming` `priority-high`  
**Assignees**: TBD  
**Estimate**: 1–2 hours

## Description

Test and document that the Flink watermark strategy correctly handles late-arriving GPS events. The GPS producer deliberately emits out-of-order events with up to 3-minute delays and periodic blackouts — the watermark must handle this correctly.

## Test Scenarios

### Test 1: Late Event Within Watermark (should be processed)
- [ ] Inject a GPS event with `timestamp` = 2 minutes ago
- [ ] Verify it is processed by Job 1 and appears in Cassandra
- [ ] Document: late event within 3-min window → accepted ✅

### Test 2: Late Event Outside Watermark (should be dropped)
- [ ] Inject a GPS event with `timestamp` = 5 minutes ago
- [ ] Verify it is **not** processed (dropped by watermark)
- [ ] Document: late event outside 3-min window → dropped ✅

### Test 3: GPS Blackout Recovery
- [ ] Use `event_injector.py` to trigger a GPS blackout for a vehicle
- [ ] After blackout ends, verify the vehicle reappears in Cassandra
- [ ] Verify no duplicate entries during recovery

### Test 4: Processing-Time vs Event-Time Comparison
- [ ] Temporarily switch to processing-time and run for 5 minutes
- [ ] Compare demand aggregation results with event-time approach
- [ ] Document the measurable difference (this proves event-time is necessary)

## Deliverables

- [ ] Test report with evidence (screenshots, query results)
- [ ] Explanation of watermark strategy chosen and why 3 minutes
- [ ] Comparison table: event-time vs processing-time results

## Acceptance Criteria

- [ ] All 4 test scenarios executed and documented
- [ ] Clear evidence that watermark handles late events correctly
- [ ] Report ready for inclusion in final technical report
