open Microsoft.Extensions.Hosting
open Microsoft.Extensions.DependencyInjection
open System
open System.Threading.Tasks
open NLog
open NLog.Extensions.Logging
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Logging.Console
open NLog.Layouts
open Microsoft.Extensions.Configuration
open NATS.Client.Core
open NATS.Net
open OpenTelemetry
open OpenTelemetry.Logs
open OpenTelemetry.Metrics
open OpenTelemetry.Trace
open DLQProcessor
open DLQService
open Mercator.HealthChecks
open ServiceDefaults

let inline configureNats (sp: IServiceProvider) =
    let loggerFactory = sp.GetRequiredService<ILoggerFactory>()
    let config = sp.GetRequiredService<IConfiguration>()
    let healthStore = sp.GetRequiredService<ServiceHealthStore>()

    let logger = loggerFactory.CreateLogger("NATS_Configurer")

    try
        logger.LogInformation "Setting up NATS connection..."

        let defaultNatsUrl =
            if Config.isRunningInDocker() then "nats://nats:4222"
            else "nats://localhost:4222"

        // Try Aspire connection string first, then custom config, then default
        let natsUrl =
            config.["ConnectionStrings:nats"]
            |> Option.ofObj
            |> Option.orElse (config |> Config.tryGetConfigValue "NatsUrl")
            |> Option.defaultValue defaultNatsUrl

        logger.LogInformation($"🔌 Connecting to NATS at: {natsUrl}")

        // A failed first connect (NATS restarting, the Pod's network not ready yet)
        // is retried by the client like any later reconnect, instead of stopping
        // the processor until the liveness probe restarts the Pod. The handshake
        // gets more than the 2 s default, as a CPU-limited Pod can be slow to start.
        let opts = NatsOpts(
            Url = natsUrl,
            Name = DLQService.Name,
            LoggerFactory = loggerFactory,
            RetryOnInitialConnect = true,
            ConnectTimeout = TimeSpan.FromSeconds 10.0
        )

        let client = new NatsClient(opts)
        // Advisories are read from a stream with a pull consumer, which has flow
        // control, so nothing here should ever overflow a core subscription's
        // pending channel. If something does, say so instead of dropping silently.
        client.Connection.add_MessageDropped(
            AsyncEventHandler<_>(fun _ args ->
                logger.LogError("NATS dropped a message on {Subject} ({Pending} pending)", args.Subject, args.Pending)
                ValueTask.CompletedTask))
        // Readiness follows the connection; liveness stays up while the client retries.
        let notConnected (reason: string) =
            healthStore.RecordResolvableError("NATS", reason)
            ValueTask.CompletedTask
        client.Connection.add_ConnectionOpened(
            AsyncEventHandler<_>(fun _ _ ->
                logger.LogInformation("Connected to NATS at {Url}.", natsUrl)
                healthStore.ClearError "NATS"
                ValueTask.CompletedTask))
        client.Connection.add_ConnectionDisconnected(
            AsyncEventHandler<_>(fun _ _ ->
                logger.LogWarning("Disconnected from NATS at {Url}; reconnecting.", natsUrl)
                notConnected "Disconnected"))
        // NATS.Client already logs each failed attempt as a warning.
        client.Connection.add_ReconnectFailed(AsyncEventHandler<_>(fun _ _ -> notConnected "Unreachable"))
        healthStore.RecordResolvableError("NATS", "Not connected yet")
        client
    with ex ->
        // Record as resolvable error - NATS might come back
        logger.LogResolvableError(healthStore, "NATS", "Failed to set up NATS connection", ex)
        raise ex

[<EntryPoint>]
let main args =
    let config = NLog.Config.LoggingConfiguration()

    let devLayout = Layouts.SimpleLayout("${logger}|${longdate}|${level:uppercase=true}|${message}|${exception:format=toString}")
    let devTarget = new NLog.Targets.ColoredConsoleTarget("console", Layout = devLayout)

    let jsonLayout = Layouts.JsonLayout(IncludeEventProperties = true, IncludeScopeProperties = true, IncludeGdc = true)
    jsonLayout.IncludeEventProperties <- true
    jsonLayout.Attributes.Add(JsonAttribute("time", "${longdate}"))
    jsonLayout.Attributes.Add(JsonAttribute("level", "${level:upperCase=true}"))
    jsonLayout.Attributes.Add(JsonAttribute("logger", "${logger}"))
    jsonLayout.Attributes.Add(JsonAttribute("message", "${message}"))
    jsonLayout.Attributes.Add(JsonAttribute("exception", "${exception:format=toString}"))
    let jsonTarget = new NLog.Targets.ConsoleTarget("jsonConsole", Layout = jsonLayout)

    // Use newer Host.CreateApplicationBuilder for Aspire compatibility
    let builder = Host.CreateApplicationBuilder(args)

    // Configure application settings
    builder.Configuration.AddJsonFile("local.settings.json", optional = true, reloadOnChange = true) |> ignore

    // Ensure environment variables are loaded (they should be by default, but be explicit)
    builder.Configuration.AddEnvironmentVariables() |> ignore

    // ServiceDefaults: health-check infrastructure and the HttpClient factory
    builder.AddServiceDefaults() |> ignore

    // Configure logging
    let env = builder.Environment.EnvironmentName
    if env = "Production" then
        config.AddTarget(jsonTarget)
        config.AddRule(LogLevel.Info, LogLevel.Fatal, jsonTarget)
        LogManager.Configuration <- config
    else
        config.AddTarget(devTarget)
        config.AddRuleForAllLevels(devTarget)
        LogManager.Configuration <- config

    // NLog is added alongside the host's default providers; Production filters
    // the console one out below.
    let minLogLevel = if env = "Production" then LogLevel.Information else LogLevel.Debug
    builder.Logging.SetMinimumLevel(minLogLevel) |> ignore
    // Reduce noisy DEBUG shutdown logs from NATS internals (outside Production;
    // there the whole NATS.Client category is raised to Warning below, and these
    // more specific rules would override it back to Information).
    if env <> "Production" then
        builder.Logging.AddFilter("NATS.Client.Core.Internal.NatsReadProtocolProcessor", LogLevel.Information) |> ignore
        builder.Logging.AddFilter("NATS.Client.Core.NatsConnection", LogLevel.Information) |> ignore
        builder.Logging.AddFilter("NATS.Client.Core.Commands.CommandWriter", LogLevel.Information) |> ignore
    // Suppress health check Debug logs in production (they run every 5-30 seconds)
    builder.Logging.AddFilter("Mercator.HealthChecks.ServiceHealthCheck", LogLevel.Information) |> ignore
    builder.Logging.AddFilter("Mercator.HealthChecks.LivenessHealthCheck", LogLevel.Information) |> ignore
    builder.Logging.AddFilter("Microsoft.Extensions.Diagnostics.HealthChecks", LogLevel.Warning) |> ignore
    if env = "Production" then
        // NLog writes every event as one JSON line. The host's default console
        // provider would print each again as plain text.
        builder.Logging.AddFilter<ConsoleLoggerProvider>(fun _ -> false) |> ignore
        // Library chatter (connection handshakes, ServerInfo dumps, hosting
        // internals) only when something is wrong; the host's three lifetime
        // lines (started, environment, content root) stay.
        builder.Logging.AddFilter("NATS.Client", LogLevel.Warning) |> ignore
        builder.Logging.AddFilter("Microsoft", LogLevel.Warning) |> ignore
        builder.Logging.AddFilter("Microsoft.Hosting.Lifetime", LogLevel.Information) |> ignore
    builder.Logging.AddNLog(NLogProviderOptions(RemoveLoggerFactoryFilter = false)) |> ignore

    // OpenTelemetry (Mercator ADR-0013): logs, metrics and traces over OTLP to the
    // collector, which fans out to Loki, Prometheus and Tempo. Nothing is exported
    // unless OTEL_EXPORTER_OTLP_ENDPOINT is set; the chart sets it together with
    // OTEL_SERVICE_NAME when global.observability is enabled. Log records pass the
    // same category filters as NLog above.
    builder.Logging.AddOpenTelemetry(fun options ->
        options.IncludeFormattedMessage <- true
        options.IncludeScopes <- true) |> ignore
    let openTelemetry =
        builder.Services.AddOpenTelemetry()
            .WithMetrics(fun metrics ->
                metrics.AddRuntimeInstrumentation().AddMeter(Telemetry.Name) |> ignore)
            .WithTracing(fun tracing ->
                tracing.AddSource(Telemetry.Name) |> ignore)
    if not (String.IsNullOrWhiteSpace builder.Configuration.["OTEL_EXPORTER_OTLP_ENDPOINT"]) then
        openTelemetry.UseOtlpExporter() |> ignore

    // Register services
    builder.Services.AddSingleton<INatsClient, NatsClient> configureNats |> ignore
    builder.Services.AddHostedService<DLQProcessor> (fun sp ->
        new DLQProcessor(builder.Environment, sp)) |> ignore

    // ✨ Add service health store (singleton)
    builder.Services.AddServiceHealthStore() |> ignore

    // ✨ Add health checks based on actual operational errors
    builder.Services.AddHealthChecks()
        .AddLivenessHealthCheck(DLQService.Name)   // Non-resolvable errors fail /alive
        .AddServiceHealthCheck(DLQService.Name)    // Any recorded error fails /health
        |> ignore

    // Kubernetes probes (/alive, /health) on port 8080: toolkit HealthProbes,
    // a probe-only Kestrel listener beside this generic host.
    builder.Services.AddHealthProbes(DLQService.Name) |> ignore

    let app = builder.Build()
    app.Run()
    0

