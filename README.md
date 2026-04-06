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

Use the Python loader to create the MinIO buckets and upload the datasets:

```powershell
python scripts\load-datasets.py
```

What the script does:

- Creates the `raw`, `curated`, `ml`, and `kafka-archive` buckets in MinIO
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