$ErrorActionPreference = "Stop"

$connectBaseUrl = "http://localhost:8083"
$connectorsUrl = "$connectBaseUrl/connectors"
$connectorName = "s3-sink-raw-events"

$minioAccessKey = if ($env:MINIO_ROOT_USER) { $env:MINIO_ROOT_USER } else { "minioadmin" }
$minioSecretKey = if ($env:MINIO_ROOT_PASSWORD) { $env:MINIO_ROOT_PASSWORD } else { "minioadmin123" }

Write-Host "Waiting for Kafka Connect REST API..."
while ($true) {
    try {
        $null = Invoke-RestMethod -Uri "$connectBaseUrl/" -Method Get
        break
    } catch {
        Start-Sleep -Seconds 3
    }
}
Write-Host "Kafka Connect is up."

Write-Host "Waiting for S3 connector plugin discovery..."
while ($true) {
    try {
        $plugins = Invoke-RestMethod -Uri "$connectBaseUrl/connector-plugins/" -Method Get
        if ($plugins.class -contains "io.confluent.connect.s3.S3SinkConnector") { break }
    } catch {
        # keep waiting
    }
    Start-Sleep -Seconds 2
}
Write-Host "S3 plugin available."

$config = @{
    "connector.class"                = "io.confluent.connect.s3.S3SinkConnector"
    "tasks.max"                      = "2"
    "topics"                         = "raw.gps,raw.trips"
    "s3.region"                      = "us-east-1"
    "s3.bucket.name"                 = "raw"
    "s3.part.size"                   = "5242880"
    "flush.size"                     = "100"
    "rotate.interval.ms"             = "60000"
    "storage.class"                  = "io.confluent.connect.s3.storage.S3Storage"
    "format.class"                   = "io.confluent.connect.s3.format.json.JsonFormat"
    "schema.generator.class"         = "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator"
    "partitioner.class"              = "io.confluent.connect.storage.partitioner.TimeBasedPartitioner"
    "path.format"                    = "YYYY-MM-dd"
    "locale"                         = "en-US"
    "timezone"                       = "UTC"
    "partition.duration.ms"          = "86400000"
    "topics.dir"                     = "kafka-archive"
    "store.url"                      = "http://minio:9000"
    "aws.access.key.id"              = $minioAccessKey
    "aws.secret.access.key"          = $minioSecretKey
    "key.converter"                  = "org.apache.kafka.connect.storage.StringConverter"
    "value.converter"                = "org.apache.kafka.connect.json.JsonConverter"
    "value.converter.schemas.enable" = "false"
}

$payload = $config | ConvertTo-Json -Depth 5
Write-Host "Upserting connector $connectorName..."
$null = Invoke-RestMethod -Uri "$connectorsUrl/$connectorName/config" -Method Put -ContentType "application/json" -Body $payload

Write-Host "Connector list:"
Invoke-RestMethod -Uri "$connectorsUrl/" -Method Get | ConvertTo-Json -Depth 5

Write-Host "Connector status:"
Invoke-RestMethod -Uri "$connectorsUrl/$connectorName/status" -Method Get | ConvertTo-Json -Depth 8

Write-Host "Done."
