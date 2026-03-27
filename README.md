# Alertum Logging SDK (Streaming Ingest)

This SDK publishes logs to a streaming queue (Kafka) for ingestion by the Alertum logs-ingestion module. It does **not** expose HTTP ingestion endpoints.

## Client Configuration (only this)

```xml
<appender name="ALERTUM" class="io.alertum.logging.AlertumAppender">
    <teamId>${ALERTUM_TEAM_ID}</teamId>
    <service>order-service</service>
    <environment>prod</environment>
</appender>
```

## Required Environment Variables

- `ALERTUM_KAFKA_BOOTSTRAP_SERVERS` (e.g. `kafka:9092`)
- `ALERTUM_KAFKA_TOPIC` (default `alertum.logs`)

## Optional Runtime Settings

- `ALERTUM_BATCH_SIZE` (default `500`)
- `ALERTUM_FLUSH_INTERVAL_MS` (default `1000`)
- `ALERTUM_QUEUE_CAPACITY` (default `5000`)
- `ALERTUM_KAFKA_ACKS` (default `1`)
- `ALERTUM_KAFKA_LINGER_MS` (default `5`)
- `ALERTUM_KAFKA_BATCH_SIZE` (default `32768`)
- `ALERTUM_KAFKA_COMPRESSION` (default `gzip`)

## Payload Schema

Each Kafka message contains a batch of log records:

```json
{
  "schemaVersion": 1,
  "logs": [
    {
      "timestamp": 1774475257024,
      "level": "INFO",
      "message": "...",
      "service": "order-service",
      "environment": "prod",
      "teamId": "TEAM_ID",
      "endpoint": "/api/orders",
      "statusCode": 200,
      "durationMs": 123,
      "error": false,
      "traceId": "...",
      "spanId": "...",
      "requestId": "..."
    }
  ]
}
```
