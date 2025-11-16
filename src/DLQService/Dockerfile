###############
# Build stage #
###############
FROM mcr.microsoft.com/dotnet/sdk:10.0-alpine AS build

# Build argument to force cache invalidation when source code changes
ARG BUILD_SHA=unknown
ARG CACHE_BUST=1

WORKDIR /src

# Copy dependency files first for better layer caching
COPY global.json ./
COPY .config ./.config/
COPY paket.dependencies paket.lock ./
COPY .paket ./.paket

# Restore dotnet tools (Paket)
RUN dotnet tool restore

# Restore Paket dependencies (all groups)
RUN dotnet paket restore

# Use build args to ensure this layer is invalidated when source changes
# This prevents Docker from reusing cached layers with old compiled code
RUN echo "Build SHA: ${BUILD_SHA}, Cache bust: ${CACHE_BUST}"

# Now copy source code (this layer will be invalidated on source changes)
COPY src ./src

WORKDIR /src/src/DLQService

RUN dotnet publish DLQService.fsproj \
    -c Release \
    -o /app/publish \
    -p:PublishTrimmed=true \
    -p:PublishSingleFile=true \
    -p:InvariantGlobalization=true \
    -p:ReadyToRun=true

################
# Runtime stage#
################
FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-alpine

ENV DOTNET_EnableDiagnostics=0 \
    DOTNET_ENVIRONMENT=Production

WORKDIR /app

COPY --from=build /app/publish ./

# Note: appsettings.Production.json is already included in the publish output
# It will be automatically loaded by .NET when DOTNET_ENVIRONMENT=Production (set above)

RUN if ! getent group app >/dev/null; then addgroup -S app; fi \
    && if ! getent passwd app >/dev/null; then adduser -S -G app app; fi
USER app

ENTRYPOINT ["./DLQService"]

