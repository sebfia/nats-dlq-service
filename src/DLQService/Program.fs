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

let inline configureNats (sp: IServiceProvider) =
    let loggerFactory = sp.GetRequiredService<ILoggerFactory>()
    let config = sp.GetRequiredService<IConfiguration>()
    
    let logger = loggerFactory.CreateLogger("NATS_Configurer")

    try
        logger.LogInformation "Setting up NATS connection..."

        let defaultNatsUrl = 
            if Config.isRunningInDocker() then "nats://nats:4222"
            else "nats://localhost:4222"

        let opts = NatsOpts(
            Url = (config |> Config.tryGetConfigValue "NatsUrl" |> Option.defaultValue defaultNatsUrl),
            Name = DLQService.Name,
            LoggerFactory = loggerFactory
        )

        let client = new NatsClient(opts)
        client
    with ex ->
        logger.LogError(ex, "❌ Failed to set up NATS connection.")
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

    Host.CreateDefaultBuilder(args)
        .ConfigureAppConfiguration(fun context configBuilder ->
            configBuilder.AddJsonFile("local.settings.json", optional = true, reloadOnChange = true) |> ignore
        )
        .ConfigureLogging(fun context logging ->
            let env = context.HostingEnvironment.EnvironmentName
            if env = "Production" then
                config.AddTarget(jsonTarget)
                config.AddRuleForAllLevels(jsonTarget)
                LogManager.Configuration <- config
            else
                config.AddTarget(devTarget)
                config.AddRuleForAllLevels(devTarget)
                LogManager.Configuration <- config

            logging.ClearProviders() |> ignore
            logging.SetMinimumLevel(LogLevel.Trace) |> ignore
            // Reduce noisy DEBUG shutdown logs from NATS internals
            logging.AddFilter("NATS.Client.Core.Internal.NatsReadProtocolProcessor", LogLevel.Information) |> ignore
            logging.AddFilter("NATS.Client.Core.NatsConnection", LogLevel.Information) |> ignore
            logging.AddFilter("NATS.Client.Core.Commands.CommandWriter", LogLevel.Information) |> ignore
            logging.AddNLog() |> ignore
        )
        .ConfigureServices(fun context services ->
            services.AddSingleton<INatsClient, NatsClient> configureNats |> ignore
            services.AddHostedService<DLQProcessor> (fun sp -> 
                new DLQProcessor(context.HostingEnvironment, sp)) |> ignore
        )
        .Build()
        .Run()
    0

