$ErrorActionPreference = 'Stop'
$Topic = 'atlasflow.cdc.v1'
Write-Host "Creating the single business topic: $Topic" -ForegroundColor Cyan
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:19092 --create --if-not-exists `
  --topic $Topic --partitions 3 --replication-factor 1 `
  --config retention.ms=259200000
if ($LASTEXITCODE -ne 0) { throw "Could not create Kafka topic $Topic." }
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list
if ($LASTEXITCODE -ne 0) { throw 'Could not list Kafka topics.' }
