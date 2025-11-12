# NATS DLQ Service

A lightweight .NET background service that centralizes dead-letter queue (DLQ) handling for NATS JetStream. Automatically captures terminated and undeliverable messages from services within your configured namespace and environment, and republishes them into a dedicated JetStream DLQ stream for later analysis or reprocessing.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![.NET](https://img.shields.io/badge/.NET-10.0-purple.svg)](https://dotnet.microsoft.com/)

## What it does

The DLQService automatically listens for two types of failed messages from services within the configured namespace and environment via NATS JetStream advisory events:

1. **Terminated messages**: Messages that services explicitly terminate by calling `AckTerminateAsync()`
   - Subscribes to: `$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.>`
   - NATS automatically publishes these advisory events when messages are terminated
   - **No action required from services** - the DLQService handles everything automatically

2. **Undeliverable messages**: Messages that exceed the consumer's `MaxDeliver` threshold
   - Subscribes to: `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>`
   - NATS automatically publishes these advisory events when messages exceed max deliveries
   - **No action required from services** - the DLQService handles everything automatically

**Message filtering**: The service validates that advisory events match the configured namespace and environment by checking the stream name (format: `{NAMESPACE}_{ENV}_*`) before processing. This ensures that:
- Each DLQService instance only handles messages from services within its designated namespace/environment
- Advisory events from streams in other namespaces or environments are safely ignored
- Cross-environment and cross-namespace contamination is prevented

**DLQ Storage**: All processed messages are published to a dedicated JetStream DLQ stream:
- Stream name: `{NAMESPACE}_{ENV}_DLQ`
- Subject used to store: `{namespace}.{env}.dlq.{stream}.{consumer}`
- Adds useful metadata as headers:
  - Copies all original headers as `X-DLQ-<OriginalHeaderName>`
  - `X-DLQ-Original-Subject`: original subject (if provided)
  - `X-DLQ-Original-Stream`: stream name
  - `X-DLQ-Original-Consumer`: consumer name
  - `X-DLQ-Failure-Reason`: `"Terminated"` or `"MaxDeliveriesExceeded"`
  - `X-DLQ-Delivery-Attempts`: number of delivery attempts (if available)
  - `X-DLQ-Failure-Timestamp`: timestamp of DLQ action (ISO-8601)

## Environment conventions

- `env` is determined by the `Environment` configuration setting (takes precedence) or falls back to `DOTNET_ENVIRONMENT`:
  - `development` or `dev` → `dev`
  - `staging` or `stage` → `staging`
  - `production` or `prod` → `prod`
- Stream names use uppercase env (e.g., `MERCATOR_PROD_DLQ`), subjects use lowercase env (e.g., `mercator.prod.dlq.*`).

## Configuration

Set the NATS URL, namespace, and environment using configuration files or environment variables.

### Local Development

- File: `src/DLQService/local.settings.json`

```json
{
  "IsEncrypted": false,
  "Namespace": "Mercator",
  "Environment": "development",
  "NatsUrl": "nats://localhost:4222",
  "ConnectionStrings": {}
}
```

### Docker/Production

- File: `src/DLQService/appsettings.Production.json` (automatically loaded when `DOTNET_ENVIRONMENT=Production`)

```json
{
  "Namespace": "Mercator",
  "Environment": "production",
  "NatsUrl": "nats://nats:4222",
  "DLQStreamReplicas": "1",
  "ConnectionStrings": {}
}
```

**Note**: `DLQStreamReplicas` defaults to `1` (compatible with non-clustered NATS). Set to `3` or higher only if your NATS server is running in clustered mode.

The Dockerfile automatically copies this file into the container. The service will load it automatically since `DOTNET_ENVIRONMENT=Production` is set in the Docker image.

### Environment Variables (override files at runtime)

- `Environment` = `development` | `staging` | `production` (or `dev` | `stage` | `prod`)
- `DOTNET_ENVIRONMENT` = `Development` | `Staging` | `Production` (fallback if `Environment` not set)
- `Namespace` = logical namespace for subjects/streams (default: `"Mercator"`)
- `NatsUrl` = `nats://<host>:<port>`
- `DLQStreamReplicas` = number of replicas for the DLQ stream (default: `1`, use `3` or higher only for clustered NATS)

## How to run locally

From the repository root or the service folder:

```bash
cd src/DLQService
dotnet build
DOTNET_ENVIRONMENT=Development dotnet run
```

Logs in Development are human-readable; in Production, they are JSON-formatted.

## How services integrate with the DLQ

**No integration required!** The DLQService automatically listens to NATS JetStream advisory events. Services don't need to publish anything or configure anything special.

### For Terminated Messages

When a service calls `AckTerminateAsync()` on a message, NATS automatically publishes an advisory event to `$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.>`. The DLQService automatically:
- Receives the advisory event
- Validates it matches the configured namespace/environment (by checking the stream name)
- Stores the metadata in the DLQ stream

**Example** (F# - no DLQ code needed):
```fsharp
// Just terminate the message - DLQService handles the rest automatically
do! msg.AckTerminateAsync()
```

### For Undeliverable Messages (Max Deliveries Exceeded)

When a message exceeds the consumer's `MaxDeliver` threshold, NATS automatically publishes an advisory event to `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>`. The DLQService automatically:
- Receives the advisory event
- Validates it matches the configured namespace/environment (by checking the stream name)
- Stores the metadata in the DLQ stream

**Example** (F# consumer configuration - no DLQ configuration needed):
```fsharp
let consumerConfig = ConsumerConfig(
    Name = "MY_CONSUMER",
    DurableName = "MY_CONSUMER",
    MaxDeliver = 5,
    // No DeliverSubject needed - DLQService handles it automatically
    // ... other config
)
```

**Important**: 
- The DLQService filters advisory events by stream name (format: `{NAMESPACE}_{ENV}_*`)
- If the DLQService is configured with `Namespace: "Mercator"` and `Environment: "development"`, it will only process advisory events for streams starting with `MERCATOR_DEV_`
- Advisory events from streams like `OTHERNAMESPACE_DEV_*` or `MERCATOR_STAGING_*` will be logged as debug messages and skipped.

## Operational notes

- The service will ensure a durable DLQ stream exists per environment: `{NAMESPACE}_{ENV}_DLQ` (file storage, S2 compression in Production).
- Messages are appended under subjects reflecting the original stream and consumer, making filtering and reprocessing straightforward.
- **Terminated messages**: NATS automatically publishes advisory events when services call `AckTerminateAsync()`. No action required from services.
- **Undeliverable messages**: NATS automatically publishes advisory events when messages exceed `MaxDeliver`. No action required from services.
- **Note**: Advisory events contain metadata (stream, consumer, subject, delivery count) but not the message payload. The DLQ stores the advisory event metadata for tracking and analysis.
- The service only processes advisory events for streams that match its configured namespace and environment, preventing cross-contamination.

## Troubleshooting

- No DLQ entries appear:
  - **For terminated messages**: Verify that services are actually calling `AckTerminateAsync()` on messages. NATS will automatically publish advisory events.
  - **For undeliverable messages**: Verify that consumers have `MaxDeliver` configured and messages are actually exceeding this threshold. NATS will automatically publish advisory events.
  - **Check that stream names match the DLQService configuration** - advisory events for streams that don't start with `{NAMESPACE}_{ENV}_` are logged as debug messages and skipped.
  - Confirm `NatsUrl` and connectivity to your NATS server.
  - Check logs; in Production they are JSON on stdout.
  - Look for debug messages like "Skipping terminated advisory event - stream doesn't match namespace/environment" which indicate filtering is working but streams don't match.
  - Verify that NATS JetStream advisory events are enabled (they are enabled by default).
- Confirm the stream exists and subjects are bound:
  - Stream: `{NAMESPACE}_{ENV}_DLQ`
  - Subjects: `{namespace}.{env}.dlq.>`
- Advisory events are received but no DLQ entries appear:
  - Check that the stream name in the advisory event matches the namespace/environment pattern (`{NAMESPACE}_{ENV}_*`).
  - Verify the advisory event JSON contains the expected fields: `stream`, `consumer`, `subject`, `delivered`.

## Build

```bash
cd src/DLQService
dotnet build
```

## Docker

Build the Docker image:

```bash
docker build -f src/DLQService/Dockerfile -t dlq-service:latest .
```

## Notes

- This service follows the same hosting and logging patterns used by `IdentityService`, adapted for background worker semantics.

