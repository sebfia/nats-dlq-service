module ServiceDefaults

open Microsoft.Extensions.Hosting
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Diagnostics.HealthChecks
open Microsoft.Extensions.Http

/// Extension methods for HostApplicationBuilder to add ServiceDefaults functionality.
/// This replaces the external aspire-servicedefaults dependency.
/// 
/// Provides observability, health checks, and service discovery configuration.
/// Note: This is a minimal implementation. If you need OpenTelemetry, Service Discovery,
/// or other Aspire features, add the corresponding NuGet packages and extend this method.
[<AutoOpen>]
module HostApplicationBuilderExtensions =
    type Microsoft.Extensions.Hosting.HostApplicationBuilder with
        /// Adds ServiceDefaults configuration for observability, health checks, and service discovery.
        /// This method provides the same interface as Aspire's ServiceDefaults but implemented locally.
        /// 
        /// Configures:
        /// - Health checks infrastructure (additional checks are added in Program.fs)
        /// - HTTP client factory for service-to-service communication
        member this.AddServiceDefaults() =
            // Configure health checks infrastructure
            // Note: Additional health checks are added in Program.fs
            // Health check server is already configured via HealthCheckServer.fs
            this.Services.AddHealthChecks() |> ignore
            
            // Configure HTTP client factory for service-to-service communication
            // This enables HttpClient dependency injection with proper lifecycle management
            this.Services.AddHttpClient() |> ignore
            
            this

