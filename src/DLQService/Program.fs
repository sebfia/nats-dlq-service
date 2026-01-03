open Microsoft.Extensions.Hosting
open Microsoft.Extensions.DependencyInjection
open System
open System.Threading.Tasks
open NLog
open NLog.Extensions.Logging
open Microsoft.Extensions.Logging
open NLog.Layouts
open Microsoft.Extensions.Configuration
open NATS.Client.Core
open NATS.Net
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

        let opts = NatsOpts(
            Url = natsUrl,
            Name = DLQService.Name,
            LoggerFactory = loggerFactory
        )

        let client = new NatsClient(opts)
        logger.LogHealthy(healthStore, "NATS", "Connection established")
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

    // 🆕 Add Aspire ServiceDefaults for observability (OpenTelemetry, health checks, service discovery)
    builder.AddServiceDefaults() |> ignore

    // Configure logging
    let env = builder.Environment.EnvironmentName
    if env = "Production" then
        config.AddTarget(jsonTarget)
        config.AddRuleForAllLevels(jsonTarget)
        LogManager.Configuration <- config
    else
        config.AddTarget(devTarget)
        config.AddRuleForAllLevels(devTarget)
        LogManager.Configuration <- config

    // NOTE: Don't clear providers! ServiceDefaults added OpenTelemetry logging for Aspire Dashboard.
    // Just add NLog alongside it.
    builder.Logging.SetMinimumLevel(LogLevel.Trace) |> ignore
    // Reduce noisy DEBUG shutdown logs from NATS internals
    builder.Logging.AddFilter("NATS.Client.Core.Internal.NatsReadProtocolProcessor", LogLevel.Information) |> ignore
    builder.Logging.AddFilter("NATS.Client.Core.NatsConnection", LogLevel.Information) |> ignore
    builder.Logging.AddFilter("NATS.Client.Core.Commands.CommandWriter", LogLevel.Information) |> ignore
    // Suppress health check Debug logs in production (they run every 5-30 seconds)
    builder.Logging.AddFilter("Mercator.HealthChecks.ServiceHealthCheck", LogLevel.Information) |> ignore
    builder.Logging.AddFilter("Mercator.HealthChecks.LivenessHealthCheck", LogLevel.Information) |> ignore
    builder.Logging.AddNLog() |> ignore

    // Register services
    builder.Services.AddSingleton<INatsClient, NatsClient> configureNats |> ignore
    builder.Services.AddHostedService<DLQProcessor> (fun sp ->
        new DLQProcessor(builder.Environment, sp)) |> ignore

    // ✨ Add service health store (singleton)
    builder.Services.AddServiceHealthStore() |> ignore

    // ✨ Add health checks based on actual operational errors
    builder.Services.AddHealthChecks()
        .AddLivenessHealthCheck()      // Checks for non-resolvable errors (affects /alive)
        .AddServiceHealthCheck()        // Checks all errors (affects /health)
        |> ignore

    // ✨ Add health check HTTP server on port 8080
    builder.Services.AddHealthCheckServer(8080) |> ignore

    let app = builder.Build()
    app.Run()
    0

