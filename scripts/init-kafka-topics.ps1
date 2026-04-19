$ErrorActionPreference = "Stop"

$bootstrap = "localhost:9092"
$retentionMs = 604800000 # 7 days

Write-Host "Creating Kafka topics..."

$createRawGps = @(
    "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
    "--bootstrap-server", $bootstrap,
    "--create", "--if-not-exists",
    "--topic", "raw.gps",
    "--partitions", "3",
    "--replication-factor", "1",
    "--config", "retention.ms=$retentionMs"
)

docker @createRawGps
Write-Host "  raw.gps (3 partitions, 7d retention)"

$createRawTrips = @(
    "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
    "--bootstrap-server", $bootstrap,
    "--create", "--if-not-exists",
    "--topic", "raw.trips",
    "--partitions", "3",
    "--replication-factor", "1",
    "--config", "retention.ms=$retentionMs"
)

docker @createRawTrips
Write-Host "  raw.trips (3 partitions, 7d retention)"

Write-Host ""
Write-Host "Verifying topics:"

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server $bootstrap --describe --topic raw.gps
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server $bootstrap --describe --topic raw.trips

Write-Host ""
Write-Host "Done - Kafka topics initialized."
