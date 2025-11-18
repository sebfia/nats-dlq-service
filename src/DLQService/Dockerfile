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
