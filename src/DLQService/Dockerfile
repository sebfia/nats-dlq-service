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
COPY paket.dependencies ./
COPY paket.lock ./
COPY .config ./.config
COPY src ./src

# Install manifest tools (Paket alpha) and restore dependencies
RUN dotnet tool restore
RUN dotnet tool run paket restore

WORKDIR /src/src/DLQService

# Use build args to ensure this layer is rebuilt on every commit
# This prevents Docker from reusing cached layers with old compiled code
RUN echo "Build ID: ${BUILD_ID}, Git SHA: ${GIT_SHA}"

# Build with no cache and force clean rebuild
# Disable incremental compilation and use single-threaded compilation to avoid state machine issues
# Note: PublishSingleFile bundles ASP.NET Core framework assemblies, so runtime-deps is sufficient
RUN dotnet publish DLQService.fsproj \
    -c Release \
    -o /app/publish \
    -p:PublishTrimmed=true \
    -p:PublishSingleFile=true \
    -p:InvariantGlobalization=true \
    -p:ReadyToRun=true \
    -p:IncludeNativeLibrariesForSelfExtract=true

################
# Runtime stage#
################
FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-alpine

# HEALTHCHECK_URLS is deliberately not set: the toolkit's probe listener
# detects DOTNET_RUNNING_IN_CONTAINER / KUBERNETES_SERVICE_HOST and binds
# http://+:8080 on its own. Set it only to override the bind address.
ENV DOTNET_EnableDiagnostics=0 \
    DOTNET_ENVIRONMENT=Production

WORKDIR /app

COPY --from=build /app/publish ./

# Health probes /alive and /health (see Program.fs / toolkit HealthProbes.fs)
EXPOSE 8080

RUN if ! getent group app >/dev/null; then addgroup -S app; fi \
    && if ! getent passwd app >/dev/null; then adduser -S -G app app; fi
USER app

ENTRYPOINT ["./DLQService"]
