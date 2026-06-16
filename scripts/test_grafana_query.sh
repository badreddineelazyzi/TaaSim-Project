#!/bin/sh
curl -s -u admin:admin123 -X POST -H "Content-Type: application/json" \
  -d '{"queries":[{"refId":"A","target":"SELECT value, label FROM taasim.kpi_aggregates WHERE kpi_name = '\''total_trips'\'' AND category = '\''total'\''","rawQuery":true,"keyspace":"taasim"}]}' \
  "http://localhost:3000/api/ds/query?ds_uid=efo5rnez8n2f4e"
