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

**Message filtering**: The service validates that messages match the configured namespace and environment by checking the **original message subject** (format: `{namespace}.{env}.>`) before processing. This ensures that:

- Each DLQService instance only handles messages from subjects within its designated namespace/environment

- Advisory events for messages with subjects outside the configured pattern are safely ignored

- Cross-environment and cross-namespace contamination is prevented

**DLQ Storage**: All processed messages are published to a dedicated JetStream DLQ stream:

- Stream name: `{NAMESPACE}_{ENV}_DLQ`

- Subject used to store: `{namespace}.{env}.dlq.{stream}.{consumer}`

- Payload: original message body fetched from the originating stream so nothing is lost

- Headers:
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

Set the NATS URL, namespace, environment, and DLQ stream properties using configuration files or environment variables.

### Local Development

- File: `src/DLQService/local.settings.json`

```json
{
  "IsEncrypted": false,
  "Namespace": "Mercator",
  "Environment": "development",
  "NatsUrl": "nats://localhost:4222",
  "DLQStream": {
    "NumReplicas": "1",
    "AllowUpdateStream": "true"
  },
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
  "DLQStream": {
    "NumReplicas": "3",
    "Retention": "Limits",
    "Storage": "File",
    "NoAck": "false",
    "Compression": "S2",
    "MaxAgeDays": "365",
    "Discard": "Old",
    "AllowDirect": "true",
    "DuplicateWindowMinutes": "2.0",
    "MaxMsgs": "-1",
    "MaxBytes": "-1",
    "MaxMsgSize": "-1",
    "MaxConsumers": "-1",
    "AllowUpdateStream": "false"
  },
  "ConnectionStrings": {}
}
```

### Configuration via Environment Variables

When running in Docker, docker-compose, or Kubernetes, you can override any configuration setting using environment variables instead of modifying config files. .NET uses a hierarchical configuration system where environment variables take precedence over configuration files.

#### Environment Variable Naming Convention

Convert JSON paths to environment variables using double underscores (`__`) as separators:

- `Namespace` → `Namespace`
- `Environment` → `Environment`
- `NatsUrl` → `NatsUrl`
- `DLQStream:NumReplicas` → `DLQStream__NumReplicas`
- `DLQStream:MaxAgeDays` → `DLQStream__MaxAgeDays`

#### Docker

```bash
docker run -d \
  --name dlq-service \
  -e DOTNET_ENVIRONMENT=Production \
  -e NatsUrl=nats://your-nats-server:4222 \
  -e Namespace=YourNamespace \
  -e Environment=production \
  -e DLQStream__NumReplicas=3 \
  -e DLQStream__MaxAgeDays=365 \
  -e DLQStream__AllowUpdateStream=false \
  ghcr.io/sebfia/dlqservice:latest
```

#### Docker Compose

```yaml
version: '3.8'
services:
  dlq-service:
    image: ghcr.io/sebfia/dlqservice:latest
    environment:
      DOTNET_ENVIRONMENT: Production
      NatsUrl: nats://nats:4222
      Namespace: Mercator
      Environment: production
      DLQStream__NumReplicas: "3"
      DLQStream__Retention: Limits
      DLQStream__Storage: File
      DLQStream__Compression: S2
      DLQStream__MaxAgeDays: "365"
      DLQStream__AllowUpdateStream: "false"
    depends_on:
      - nats
    restart: unless-stopped
```

#### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dlq-service
spec:
  replicas: 1
  selector:
    matchLabels:
      app: dlq-service
  template:
    metadata:
      labels:
        app: dlq-service
    spec:
      containers:
      - name: dlq-service
        image: ghcr.io/sebfia/dlqservice:latest
        env:
        - name: DOTNET_ENVIRONMENT
          value: "Production"
        - name: NatsUrl
          value: "nats://nats.nats-system.svc.cluster.local:4222"
        - name: Namespace
          value: "Mercator"
        - name: Environment
          value: "production"
        - name: DLQStream__NumReplicas
          value: "3"
        - name: DLQStream__Retention
          value: "Limits"
        - name: DLQStream__Storage
          value: "File"
        - name: DLQStream__Compression
          value: "S2"
        - name: DLQStream__MaxAgeDays
          value: "365"
        - name: DLQStream__AllowUpdateStream
          value: "false"
```

**Using ConfigMaps in Kubernetes:**

Option 1: Using `envFrom` to inject ConfigMap as environment variables:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: dlq-service-config
data:
  NatsUrl: "nats://nats.nats-system.svc.cluster.local:4222"
  Namespace: "Mercator"
  Environment: "production"
  DLQStream__NumReplicas: "3"
  DLQStream__MaxAgeDays: "365"
  DLQStream__AllowUpdateStream: "false"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dlq-service
spec:
  replicas: 1
  selector:
    matchLabels:
      app: dlq-service
  template:
    metadata:
      labels:
        app: dlq-service
    spec:
      containers:
      - name: dlq-service
        image: ghcr.io/sebfia/dlqservice:latest
        env:
        - name: DOTNET_ENVIRONMENT
          value: "Production"
        envFrom:
        - configMapRef:
            name: dlq-service-config
```

Option 2: Mounting ConfigMap as `appsettings.json` file:

This approach mounts a ConfigMap containing a complete JSON configuration file directly into the application directory, allowing you to manage configuration in a more structured way without using environment variables.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: dlq-service-appsettings
data:
  appsettings.json: |
    {
      "Namespace": "Mercator",
      "Environment": "production",
      "NatsUrl": "nats://nats.nats-system.svc.cluster.local:4222",
      "DLQStream": {
        "NumReplicas": "3",
        "Retention": "Limits",
        "Storage": "File",
        "NoAck": "false",
        "Compression": "S2",
        "MaxAgeDays": "365",
        "Discard": "Old",
        "AllowDirect": "true",
        "DuplicateWindowMinutes": "2.0",
        "MaxMsgs": "-1",
        "MaxBytes": "-1",
        "MaxMsgSize": "-1",
        "MaxConsumers": "-1",
        "AllowUpdateStream": "false"
      }
    }
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dlq-service
spec:
  replicas: 1
  selector:
    matchLabels:
      app: dlq-service
  template:
    metadata:
      labels:
        app: dlq-service
    spec:
      containers:
      - name: dlq-service
        image: ghcr.io/sebfia/dlqservice:latest
        env:
        - name: DOTNET_ENVIRONMENT
          value: "Production"
        volumeMounts:
        - name: config-volume
          mountPath: /app/appsettings.json
          subPath: appsettings.json
          readOnly: true
      volumes:
      - name: config-volume
        configMap:
          name: dlq-service-appsettings
```

**Key points when mounting as a file:**
- The `volumeMounts.mountPath` is set to `/app/appsettings.json` (the application directory in the Docker image)
- Using `subPath: appsettings.json` ensures only the specific file is mounted, not the entire directory
- The ConfigMap data key must match the `subPath` value
- This file will be loaded automatically by .NET's configuration system alongside `appsettings.Production.json`
- Configuration precedence: `appsettings.json` < `appsettings.Production.json` < environment variables
- To override the production settings file entirely, mount to `/app/appsettings.Production.json` instead

### Configuration Options

#### General Settings

- `Environment` = `development` | `staging` | `production` (or `dev` | `stage` | `prod`)
- `DOTNET_ENVIRONMENT` = `Development` | `Staging` | `Production` (fallback if `Environment` not set)
- `Namespace` = logical namespace for subjects/streams (default: `"Mercator"`)
- `NatsUrl` = `nats://<host>:<port>`

#### DLQ Stream Configuration (`DLQStream` section)

All properties are optional and will use the defaults if not specified:

- **`NumReplicas`** (default: `1`) - Number of replicas for the DLQ stream. Set to `3` or higher only for clustered NATS.

- **`Retention`** (default: `Limits`) - Stream retention policy
  - `Limits` - Retain based on limits (MaxMsgs, MaxBytes, MaxAge)
  - `Interest` - Retain while there are consumers
  - `Workqueue` - Messages removed after acknowledgment

- **`Storage`** (default: `File`) - Storage backend
  - `File` - Persistent file storage
  - `Memory` - In-memory storage (faster but not persistent)

- **`NoAck`** (default: `false`) - Disable message acknowledgments

- **`Compression`** (default: `S2`) - Compression algorithm
  - `S2` - S2 compression (recommended)
  - `None` - No compression

- **`MaxAgeDays`** (default: `365`) - Maximum age of messages in days. Set to `0` for unlimited.

- **`Discard`** (default: `Old`) - Discard policy when limits are reached
  - `Old` - Discard oldest messages
  - `New` - Reject new messages

- **`AllowDirect`** (default: `true`) - Allow direct access to messages for consumers

- **`DuplicateWindowMinutes`** (default: `2.0`) - Window for duplicate message detection in minutes

- **`MaxMsgs`** (default: `-1`) - Maximum number of messages. `-1` for unlimited.

- **`MaxBytes`** (default: `-1`) - Maximum total bytes. `-1` for unlimited.

- **`MaxMsgSize`** (default: `-1`) - Maximum message size in bytes. `-1` for unlimited.

- **`MaxConsumers`** (default: `-1`) - Maximum number of consumers. `-1` for unlimited.

- **`AllowUpdateStream`** (default: `true`) - **IMPORTANT:** When set to `false`, prevents any updates to the DLQ stream configuration after initial creation. Use this in production to lock down the stream configuration.

**Note**: The Dockerfile automatically copies `appsettings.Production.json` into the container. The service will load it automatically since `DOTNET_ENVIRONMENT=Production` is set in the Docker image.

## How to run locally

From the repository root or the service folder:

```bash
dotnet tool restore        # installs the Paket alpha CLI from the manifest
dotnet paket restore       # restores locked dependencies via Paket
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
- Fetches the original message from the stream
- Validates the original message subject matches the configured namespace/environment pattern (e.g., `mercator.dev.>`)
- If it matches, republishes the message to the DLQ with DLQ metadata headers
- If it doesn't match, skips the message and logs it

**Example** (F# - no DLQ code needed):

```fsharp
// Just terminate the message - DLQService handles the rest automatically
do! msg.AckTerminateAsync()
```

### For Undeliverable Messages (Max Deliveries Exceeded)

When a message exceeds the consumer's `MaxDeliver` threshold, NATS automatically publishes an advisory event to `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>`. The DLQService automatically:

- Receives the advisory event
- Fetches the original message from the stream
- Validates the original message subject matches the configured namespace/environment pattern (e.g., `mercator.dev.>`)
- If it matches, republishes the message to the DLQ with DLQ metadata headers
- If it doesn't match, skips the message and logs it

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

- The DLQService filters messages by checking the **original message subject** against the pattern `{namespace}.{env}.>` (e.g., `mercator.dev.>`)
- If the DLQService is configured with `Namespace: "Mercator"` and `Environment: "development"`, it will only process messages with subjects starting with `mercator.dev.`
- Messages with subjects like `othernamespace.dev.*` or `mercator.staging.*` will be logged and skipped

## Operational notes

- The service will ensure a durable DLQ stream exists per environment: `{NAMESPACE}_{ENV}_DLQ`.
- **Stream configuration**: All stream properties (retention, storage, compression, limits, etc.) are fully configurable via the `DLQStream` configuration section. If not specified, sensible defaults are used.
- **Production safety**: Set `AllowUpdateStream: false` in production to prevent accidental modifications to the DLQ stream configuration after initial creation. Once the stream exists, the service will skip updates when this is `false`.
- **Development flexibility**: Keep `AllowUpdateStream: true` in development to allow iterative configuration changes during testing.
- Messages are appended under subjects reflecting the original stream and consumer, making filtering and reprocessing straightforward.
- **Terminated messages**: NATS automatically publishes advisory events when services call `AckTerminateAsync()`. No action required from services.
- **Undeliverable messages**: NATS automatically publishes advisory events when messages exceed `MaxDeliver`. No action required from services.
- **Payload handling**: When an advisory event is received, the service fetches the original message from the stream, validates its subject matches the configured namespace/environment pattern, and if valid, republishes the exact payload into the DLQ stream alongside the metadata.
- The service only processes messages whose subjects match the configured namespace and environment pattern (`{namespace}.{env}.>`), preventing cross-contamination.

## Troubleshooting

- No DLQ entries appear:
  - **For terminated messages**: Verify that services are actually calling `AckTerminateAsync()` on messages. NATS will automatically publish advisory events.
  - **For undeliverable messages**: Verify that consumers have `MaxDeliver` configured and messages are actually exceeding this threshold. NATS will automatically publish advisory events.
  - **Check that message subjects match the DLQService configuration** - messages with subjects that don't start with `{namespace}.{env}.` are logged and skipped.
  - Confirm `NatsUrl` and connectivity to your NATS server.
  - Check logs; in Production they are JSON on stdout.
  - Look for error messages like "Skipping message from subject '...' (does not match namespace pattern '...')" which indicate filtering is working but subjects don't match.
  - Verify that NATS JetStream advisory events are enabled (they are enabled by default).
- Stream configuration not updating:
  - Check if `AllowUpdateStream` is set to `false` in your configuration. This is intentional for production safety.
  - Review logs for messages like "DLQ stream already exists and AllowUpdateStream is false. Skipping update."
  - If you need to update stream configuration in production, temporarily set `AllowUpdateStream: true`, restart the service, then set it back to `false`.
- Confirm the stream exists and subjects are bound:
  - Stream: `{NAMESPACE}_{ENV}_DLQ`
  - Subjects: `{namespace}.{env}.dlq.>`
  - Use NATS CLI: `nats stream info {NAMESPACE}_{ENV}_DLQ`
- Advisory events are received but no DLQ entries appear:
  - Check that the original message subject matches the namespace/environment pattern (`{namespace}.{env}.>`).
  - Verify the advisory event JSON contains the expected fields: `stream`, `consumer_seq`, `stream_seq`, `deliveries`.

## Build

```bash
dotnet tool restore
dotnet paket restore
cd src/DLQService
dotnet build
```

## Package Management

- Package resolution is handled by [`paket`](https://fsprojects.github.io/Paket/) (alpha channel) instead of NuGet.
- The manifest at `.config/dotnet-tools.json` pins `paket` to `10.0.0-alpha011`; run `dotnet tool restore` after cloning.
- Dependencies live in `paket.dependencies`/`paket.lock`, while per-project requirements continue to use `paket.references`.
- MSBuild hooks (`.paket/Paket.Restore.props|targets`) ensure `dotnet build`/`dotnet publish` automatically invoke Paket.

## Docker

### Pull from GitHub Container Registry

Pre-built images are automatically published to GitHub Container Registry (GHCR) on every push to `main`:

```bash
# Pull latest version
docker pull ghcr.io/sebfia/dlqservice:latest

# Pull specific version (current: 0.6.5)
docker pull ghcr.io/sebfia/dlqservice:0.6.5

# Pull by git commit SHA
docker pull ghcr.io/sebfia/dlqservice:<git-sha>
```

Run the image (see [Configuration via Environment Variables](#configuration-via-environment-variables) for complete examples):

```bash
docker run -d \
  --name dlq-service \
  -e DOTNET_ENVIRONMENT=Production \
  -e NatsUrl=nats://your-nats-server:4222 \
  -e Namespace=YourNamespace \
  -e Environment=production \
  -e DLQStream__NumReplicas=3 \
  -e DLQStream__AllowUpdateStream=false \
  ghcr.io/sebfia/dlqservice:latest
```

### Build locally

Build the Docker image from source:

```bash
docker build -f src/DLQService/Dockerfile -t dlq-service:latest .
```

## Notes

- This service follows the same hosting and logging patterns used by `IdentityService`, adapted for background worker semantics.
