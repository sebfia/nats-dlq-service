# Copilot Instructions for NATS DLQ Service

## Project Overview

This is a lightweight .NET background service written in F# that centralizes dead-letter queue (DLQ) handling for NATS JetStream. The service automatically captures terminated and undeliverable messages from services within a configured namespace and environment, and republishes them into a dedicated JetStream DLQ stream for later analysis or reprocessing.

## Technology Stack

- **Language**: F# (.NET 10.0)
- **Package Manager**: Paket
- **Key Dependencies**:
  - NATS.Net - NATS client for .NET
  - IcedTasks - Task computation expressions for F#
  - Microsoft.Extensions.Hosting - Background service hosting
  - NLog - Logging framework

## Architecture

### Core Components

1. **DLQProcessor.fs**: Main processing logic for handling NATS advisory events
2. **Program.fs**: Application entry point and service configuration
3. **ConfigurationHelpers.fs**: Configuration parsing and validation
4. **Operators.fs**: Custom F# operators and utilities

### Message Flow

1. Service listens to NATS advisory events:
   - `$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.>` - Terminated messages
   - `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>` - Undeliverable messages
2. Validates message subject matches `{namespace}.{env}.>` pattern
3. Fetches original message from source stream
4. Republishes to DLQ stream with metadata headers

### DLQ Stream Naming Convention

- **Stream name**: `{NAMESPACE}_{ENV}_DLQ` (uppercase)
- **Subject pattern**: `{namespace}.{env}.dlq.{stream}.{consumer}` (lowercase)

## Development Setup

### Prerequisites

- .NET SDK 10.0.0 or later
- NATS server with JetStream enabled (for local testing)

### Building

```bash
# Restore dotnet tools (Paket)
dotnet tool restore

# Install dependencies
dotnet paket install

# Build the project
dotnet build src/DLQService/DLQService.fsproj
```

### Running Locally

```bash
cd src/DLQService
DOTNET_ENVIRONMENT=Development dotnet run
```

Configuration is loaded from `local.settings.json` in development.

### Docker Build

```bash
docker build -f src/DLQService/Dockerfile -t dlq-service:latest .
```

## Configuration

### Environment Normalization

The service normalizes environment names:
- `development` or `dev` → `dev`
- `staging` or `stage` → `staging`
- `production` or `prod` → `prod`

Stream names use uppercase env (e.g., `MERCATOR_PROD_DLQ`), subjects use lowercase (e.g., `mercator.prod.dlq.*`).

### Key Configuration Files

- `local.settings.json` - Local development configuration
- `appsettings.Production.json` - Production configuration (loaded via DOTNET_ENVIRONMENT)
- `paket.dependencies` - NuGet package dependencies
- `paket.lock` - Locked dependency versions

## Code Style and Conventions

### F# Conventions

- Use computation expressions (e.g., `task`, `cancellableTask`, `valueTask`)
- Prefer immutable data structures
- Use pattern matching for control flow
- Follow functional programming principles

### Logging

- Development: Human-readable console output
- Production: JSON-formatted logs to stdout
- Use structured logging with NLog

### Error Handling

- Validate message subjects against namespace/environment patterns
- Log and skip messages that don't match the configured pattern
- Handle NATS connection failures gracefully

## Important Patterns

### Message Filtering

Always validate that messages match the configured namespace and environment:
```fsharp
if originalSubject.StartsWith($"{namespace}.{env}.", StringComparison.OrdinalIgnoreCase) then
    // Process message
else
    // Log and skip
```

### DLQ Headers

All DLQ messages include these headers:
- `X-DLQ-Original-Subject` - Original message subject
- `X-DLQ-Original-Stream` - Stream name
- `X-DLQ-Original-Consumer` - Consumer name
- `X-DLQ-Failure-Reason` - "Terminated" or "MaxDeliveriesExceeded"
- `X-DLQ-Delivery-Attempts` - Number of delivery attempts
- `X-DLQ-Failure-Timestamp` - ISO-8601 timestamp
- All original headers prefixed with `X-DLQ-`

## CI/CD

### GitHub Actions Workflow

The `.github/workflows/build.yml` workflow:
1. Restores .NET tools and Paket packages (with caching)
2. Builds the project in Release mode
3. Builds multi-platform Docker images (linux/amd64, linux/arm64)
4. Pushes to GitHub Container Registry (ghcr.io)
5. Creates GitHub releases with version tags

### Versioning

- Version is stored in `src/DLQService/VERSION`
- Auto-incremented on each build to main branch
- Tags follow format: `dlq-service-v{version}`

## Testing

Currently, there is no test infrastructure in this repository. When adding tests:
- Use FsUnit or Expecto for F# testing
- Mock NATS connections for unit tests
- Consider integration tests with a local NATS server

## Common Tasks

### Adding a New NuGet Package

```bash
# Edit paket.dependencies to add the package
# Then run:
dotnet paket install
```

### Changing Configuration

- Development: Edit `src/DLQService/local.settings.json`
- Production: Edit `src/DLQService/appsettings.Production.json`
- Stream config changes: Modify `DLQStream` section
- Set `AllowUpdateStream: false` in production to prevent accidental changes

### Debugging

- Set breakpoints in F# code
- Use structured logging to trace message flow
- Check NATS advisory event subjects match expectations
- Verify original message subjects match namespace/environment pattern

## Security Considerations

- Never commit secrets to `local.settings.json` or configuration files
- Use environment variables for sensitive configuration in production
- The service filters messages by subject pattern to prevent cross-environment contamination
- DLQ streams are namespaced to prevent conflicts

## Additional Resources

- [NATS JetStream Documentation](https://docs.nats.io/nats-concepts/jetstream)
- [F# Language Reference](https://docs.microsoft.com/en-us/dotnet/fsharp/)
- [Paket Package Manager](https://fsprojects.github.io/Paket/)
