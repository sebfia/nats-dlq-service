###############
# Build stage #
###############
FROM mcr.microsoft.com/dotnet/sdk:10.0-alpine AS build

# Build argument to force cache invalidation
ARG BUILD_ID=1
ARG GIT_SHA=unknown

WORKDIR /src

# Copy dependency files first for better layer caching
COPY global.json ./
COPY .config ./.config/
COPY paket.dependencies paket.lock ./

# Restore dotnet tools (Paket)
RUN dotnet tool restore

# Clear any residual caches and restore Paket dependencies (all groups)
# This will create the .paket directory automatically
RUN dotnet nuget locals all --clear && dotnet paket restore

# Copy source code
COPY src ./src

WORKDIR /src/src/DLQService

# Use build args to ensure this layer is rebuilt on every commit
# This prevents Docker from reusing cached layers with old compiled code
RUN echo "Build ID: ${BUILD_ID}, Git SHA: ${GIT_SHA}"

# Remove any residual bin/obj folders to ensure clean build
RUN find /src -type d \( -name bin -o -name obj \) -exec rm -rf {} + || true

# Build with no cache and force clean rebuild
RUN dotnet clean DLQService.fsproj --configuration Release || true && \
    dotnet publish DLQService.fsproj \
    -c Release \
    -o /app/publish \
    --no-incremental \
    -p:PublishTrimmed=true \
    -p:PublishSingleFile=true \
    -p:InvariantGlobalization=true \
    -p:ReadyToRun=true \
    -p:UseSharedCompilation=false

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

