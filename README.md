# TaaSim Project

Milestone 1 Issue 1 is implemented with a local Docker Compose stack that includes Kafka (KRaft), MinIO, Cassandra, Flink, Spark, and Grafana.

## Prerequisites

- Docker Desktop 4.x or newer
- Docker Compose V2 (`docker compose` command)
- At least 8 GB RAM available to Docker

Windows note:
- In Docker Desktop, set memory to 8 GB or higher.
- Run commands from PowerShell in this project root.

## Start the stack

```powershell
docker compose up -d
docker compose ps
```

## Service endpoints

- Kafka bootstrap: `localhost:9092`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- Cassandra CQL: `localhost:9042`
- Flink UI: `http://localhost:8081`
- Spark Master UI: `http://localhost:8080`
- Grafana: `http://localhost:3000` (admin/admin123)

## Health checks (acceptance validation)

From Git Bash, WSL, or any shell with `bash`:

```bash
chmod +x scripts/healthcheck-stack.sh
./scripts/healthcheck-stack.sh
```

Manual checks:

```powershell
docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
docker run --rm --network taasim-net minio/mc:latest sh -c "mc alias set local http://minio:9000 minioadmin minioadmin123 && mc ls local"
docker compose exec cassandra cqlsh -e "DESCRIBE KEYSPACES;"
docker compose exec flink-jobmanager flink list
docker compose exec spark-master spark-shell --version
curl http://localhost:3000/api/health
```

## S3A configuration details

- Flink containers download `hadoop-aws` and AWS SDK bundle at startup and expose MinIO through `s3a://` settings.
- Spark master/worker containers download the same jars and set `spark.hadoop.fs.s3a.*` options for MinIO.

## Stop and cleanup

```powershell
docker compose down
```

To remove all persisted data:

```powershell
docker compose down -v
```

## Milestone 1 Issue 2: Dataset Load

If you already uploaded Porto and NYC files manually to MinIO (`raw/porto-trips/` and `raw/nyc-tlc/`), you can skip this step.

Use the Python loader to create the MinIO buckets and upload the datasets:

```powershell
python scripts\load-datasets.py
```

What the script does:

- Creates the `raw`, `curated`, `ml-data` (or `ML_BUCKET` override), and `kafka-archive` buckets in MinIO
- Downloads the Porto competition data through the Kaggle CLI
- Downloads 3 months of NYC TLC Yellow Taxi parquet files
- Uploads Porto CSV files to `raw/porto-trips/`
- Uploads NYC TLC parquet files to `raw/nyc-tlc/`
- Verifies the uploads with `mc ls`

Prerequisites:

- Docker stack from Issue 1 must be running
- Kaggle CLI must be installed and authenticated with `kaggle.json`

Optional `.env` support:

- Copy `.env.example` to `.env`
- Fill in your Kaggle credentials and any overrides you want
- The loader reads `.env` automatically from the project root
- Docker Compose also reads `.env` automatically for port and credential overrides

Supported environment variables:

- `KAGGLE_USERNAME`
- `KAGGLE_KEY`
- `KAGGLE_COMPETITION`
- `NYC_MONTHS` as a space-separated list
- `DOWNLOAD_ROOT`
- `MINIO_ALIAS`
- `MINIO_ENDPOINT`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `ML_BUCKET` (`ml-data` is used because MinIO bucket names must be at least 3 characters)
- `MC_IMAGE`

Compose-side variables you can override in the same file:

- `KAFKA_PORT`
- `KAFKA_CLUSTER_ID`
- `MINIO_PORT`
- `MINIO_CONSOLE_PORT`
- `MINIO_ROOT_USER`
- `MINIO_ROOT_PASSWORD`
- `CASSANDRA_CQL_PORT`
- `SPARK_MASTER_PORT`
- `SPARK_MASTER_UI_PORT`
- `SPARK_DAEMON_MEMORY`
- `SPARK_WORKER_MEMORY`
- `SPARK_WORKER_CORES`
- `FLINK_PARALLELISM_DEFAULT`
- `GRAFANA_PORT`
- `GRAFANA_ADMIN_USER`
- `GRAFANA_ADMIN_PASSWORD`

If you want different NYC months, pass them explicitly:

```powershell
python scripts\load-datasets.py --nyc-months 2024-04 2024-05 2024-06
```

Low-bandwidth fallback (small sample datasets to unblock next issues):

```powershell
python scripts\load-datasets.py --quick-mode
```

## Milestone 1 Issue 3: Porto Profiling Notebook (Handoff)

Notebook path:

- `notebooks/issue-3-porto-profiling.ipynb`

Current status:

- Issue 3 analysis sections are complete and aligned with the checklist.
- Sections `4.1` and `5.1` are now ordered correctly (heading first, code directly below).
- Notebook was validated on quick-mode sample data.

What is covered in the notebook:

- Schema exploration (columns, dtypes, nulls, unique counts)
- Statistical profiling (duration, call type percentages, missing data, trips per taxi)
- Temporal profiling (trips/hour, day-of-week, weekend split, active taxis/hour, Friday 12-14 check, peak windows)
- Spatial profiling (polyline parsing, start/end points, average trip distance, top origin/destination areas)
- Required charts (demand curve, duration histogram, call type pie, origin density heatmap)
- Key findings summary and notes

Run with full data later:

- The same notebook works for full Porto CSV files.
- Prefer keeping only full CSV files in `data/downloads/porto` to avoid selecting `porto_sample.csv` by mistake.
- Re-run all notebook cells from top after full ingestion.

## Milestone 1 Issue 4: Porto -> Casablanca Zone Remapping

Implemented artifacts:

- `scripts/generate_zone_mapping.py`
- `scripts/issue4_zone_remapper.py`
- `data/zone_mapping.csv`
- `notebooks/spark_porto_casa.ipynb`

Generate the zone reference table:

```powershell
python scripts\generate_zone_mapping.py
```

Run the Spark remapping job from the Spark master container (recommended):

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit \
	--master spark://spark-master:7077 \
	/opt/spark/work-dir/scripts/issue4_zone_remapper.py \
	--input-path s3a://raw/porto-trips/ \
	--output-path s3a://curated/casablanca-trips-remapped/ \
	--s3-endpoint http://minio:9000 \
	--s3-access-key minioadmin \
	--s3-secret-key minioadmin123 \
	--zone-mapping-path data/zone_mapping.csv
```

Quick validation:

- Open `notebooks/spark_porto_casa.ipynb` and run all cells.
- Confirm remapped coordinates (`casa_lat`, `casa_lon`) are produced and plotted.
- Confirm output parquet exists under `s3a://curated/casablanca-trips-remapped/`.

## Milestone 1 Issue 5: Kafka Producers and Event Injection

Implemented artifacts:

- `scripts/init-kafka-topics.ps1`
- `scripts/vehicle_gps_producer.py`
- `scripts/trip_request_producer.py`
- `scripts/event_injector.py`

Initialize Kafka topics (`raw.gps`, `raw.trips`):

```powershell
powershell -ExecutionPolicy Bypass -File scripts\init-kafka-topics.ps1
```

Run GPS producer:

```powershell
python scripts\vehicle_gps_producer.py --kafka localhost:9092 --topic raw.gps --speed 10 --sample-ratio 0.3 --seed 42
```

Run trip request producer:

```powershell
python scripts\trip_request_producer.py --kafka localhost:9092 --topic raw.trips --rate 2 --duration 120 --late-prob 0.03 --seed 42
```

Inject anomalies (examples):

```powershell
python scripts\event_injector.py --kafka localhost:9092 demand_spike --zone 3 --factor 3.0 --duration 120
python scripts\event_injector.py --kafka localhost:9092 gps_blackout --taxis TAXI-001,TAXI-002 --duration 60
python scripts\event_injector.py --kafka localhost:9092 rain_event --factor 1.4 --duration 120
```

Verify events in Kafka:

```powershell
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic raw.gps --from-beginning --max-messages 5
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic raw.trips --from-beginning --max-messages 5
```

## Milestone 2 Issue 6: MinIO Layout and Kafka Connect S3 Sink

Implemented artifacts:

- `scripts/init-kafka-connectors.ps1`
- `docker-compose.yml` service `kafka-connect`

Current connector behavior:

- Connector class: `io.confluent.connect.s3.S3SinkConnector`
- Topics mirrored: `raw.gps`, `raw.trips`
- Target bucket: `raw`
- Target prefix: `kafka-archive/<topic>/YYYY-MM-dd/`
- Format: JSON

Apply connector configuration:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\init-kafka-connectors.ps1
```

Verify connector:

```powershell
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/s3-sink-raw-events/status
```

Verify archived files in MinIO (after producers run for a few minutes):

```powershell
docker run --rm --network taasim-net minio/mc:latest sh -c "mc alias set local http://minio:9000 minioadmin minioadmin123 && mc ls --recursive local/raw/kafka-archive"
```

## Milestone 2 Issue 7: Cassandra Schema and Sample Data

Implemented artifacts:

- `scripts/init_cassandra.cql`
- `scripts/insert_samples_cassandra.cql`
- `scripts/querys_cassandra.cql`

Create keyspace and tables:

```powershell
cmd /c "docker exec -i taasim-cassandra cqlsh < scripts\init_cassandra.cql"
```

Insert sample rows:

```powershell
cmd /c "docker exec -i taasim-cassandra cqlsh < scripts\insert_samples_cassandra.cql"
```

Run verification queries:

```powershell
cmd /c "docker exec -i taasim-cassandra cqlsh < scripts\querys_cassandra.cql"
```

Manual schema check:

```powershell
docker compose exec cassandra cqlsh -e "DESCRIBE KEYSPACE taasim;"
```

- ### Cassandra Schema Design Justification

To ensure high performance and avoid unbounded partitions (hotspots), the tables were designed following NoSQL query-driven principles:

1. **`vehicle_positions` Table:**
   - **Partition Key:** `(city, zone_id)`
   - **Clustering Key:** `event_time DESC`
   - **Justification:** The primary query pattern is fetching all available vehicles in a specific zone in real-time. Partitioning by city and zone ensures this data is localized on the same node for ultra-fast retrieval.

2. **`trips` Table:**
   - **Partition Key:** `(city, date_bucket)`
   - **Clustering Key:** `created_at DESC`
   - **Justification:** The primary query is fetching trip history. Partitioning by city alone would cause a massive hotspot as data grows unboundedly. Adding a `date_bucket` (e.g., 'YYYY-MM-DD') ensures partitions remain manageable and balanced across the cluster.

3. **`demand_zones` Table:**
   - **Partition Key:** `(city, zone_id)`
   - **Clustering Key:** `window_start DESC`
   - **Justification:** Supports the live demand heatmap by allowing instant querying of vehicle and request ratios per specific geographic zone.

## Milestone 2 Issue 8: Architecture Decision Record (ADR)

Status: completed.

Deliverable:

- `ADR-001.md`

Summary:

- ADR-001 documents the accepted architecture choices for TaaSim.
- It covers the 5 required decision areas: Kappa architecture, storage layer choices, Cassandra partition key design, Flink vs Spark separation, and MinIO bucket structure.
- It also includes consequences, trade-offs, and risk mitigations.