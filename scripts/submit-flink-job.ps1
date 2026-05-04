# Final attempt at a simple submission script
$ErrorActionPreference = "Stop"

$jar = "flink/gps-normalizer/target/gps-normalizer-1.0.jar"
$target = "/opt/flink/usrlib/gps-normalizer-1.0.jar"

Write-Host "Creating topic..."
docker exec taasim-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic processed.gps --partitions 3 --replication-factor 1

Write-Host "Creating dir..."
docker exec taasim-flink-jobmanager mkdir -p /opt/flink/usrlib

Write-Host "Copying jar..."
docker cp $jar taasim-flink-jobmanager:$target

Write-Host "Submitting..."
docker exec taasim-flink-jobmanager flink run -d $target

Write-Host "Done."
