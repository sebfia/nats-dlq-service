module DLQProcessor

open System
open System.Threading
open System.Threading.Tasks
open System.Text.Json
open IcedTasks
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.DependencyInjection
open NATS.Client.JetStream
open NATS.Client.JetStream.Models
open NATS.Client.Core
open NATS.Net
open DLQService
open Microsoft.Extensions.Configuration
open Config

/// DLQ message envelope containing metadata about the failed message
type DLQMessage = {
    OriginalSubject: string
    OriginalStream: string
    OriginalConsumer: string
    OriginalData: ReadOnlyMemory<byte>
    OriginalHeaders: NatsHeaders option
    FailureReason: string
    FailureTimestamp: DateTimeOffset
    DeliveryAttempts: int option
    Metadata: NatsJSMsgMetadata option
}

/// Creates or updates the DLQ stream
let inline createOrUpdateDLQStream (js: INatsJSContext) (nsUpper: string) (nsLower: string) (env: string) (numReplicas: int) = cancellableTask {
    try
        let! ct = CancellableTask.getCancellationToken()
        
        let streamName = $"{nsUpper}_{env}_DLQ"
        let subject = $"{nsLower}.{env.ToLowerInvariant()}.dlq.>"
        
        let config = StreamConfig(
            name = streamName,
            subjects = [| subject |],
            Retention = StreamConfigRetention.Limits,
            Storage = StreamConfigStorage.File,
            NoAck = false,
            Compression = StreamConfigCompression.S2,
            NumReplicas = numReplicas,
            MaxAge = TimeSpan.FromDays 365.,
            Discard = StreamConfigDiscard.Old,
            AllowDirect = true,
            DuplicateWindow = TimeSpan.FromMinutes 2.0
        )
        
        let! stream = js.CreateOrUpdateStreamAsync(config, ct)
        return Ok stream
    with ex -> 
        return Error (sprintf "Failed to create or update DLQ stream: %s" ex.Message)
}

/// Publishes a message to the DLQ stream
let inline publishToDLQ (js: INatsJSContext) (nsLower: string) (env: string) (dlqMsg: DLQMessage) = cancellableTask {
    try
        let! ct = CancellableTask.getCancellationToken()
        
        let subject = $"{nsLower}.{env.ToLowerInvariant()}.dlq.{dlqMsg.OriginalStream}.{dlqMsg.OriginalConsumer}"
        
        let headers = 
            match dlqMsg.OriginalHeaders with
            | Some h -> h
            | None -> NatsHeaders()
        
        // Copy all original headers as X-DLQ-<HeaderName>
        dlqMsg.OriginalHeaders
        |> Option.iter (fun original ->
            for h in original do
                let key = h.Key
                let valueStr = h.Value.ToString()
                headers.Add($"X-DLQ-{key}", valueStr))
        
        // Add DLQ metadata to headers
        headers.Add("X-DLQ-Original-Subject", dlqMsg.OriginalSubject)
        headers.Add("X-DLQ-Original-Stream", dlqMsg.OriginalStream)
        headers.Add("X-DLQ-Original-Consumer", dlqMsg.OriginalConsumer)
        headers.Add("X-DLQ-Failure-Reason", dlqMsg.FailureReason)
        headers.Add("X-DLQ-Failure-Timestamp", dlqMsg.FailureTimestamp.ToString("O"))
        dlqMsg.DeliveryAttempts |> Option.iter (fun attempts -> headers.Add("X-DLQ-Delivery-Attempts", string attempts))
        
        let! pubResult = js.TryPublishAsync(subject, dlqMsg.OriginalData, headers = headers, cancellationToken = ct)
        
        match pubResult.Success with
        | true -> return Ok pubResult.Value.Seq
        | false -> return Error pubResult.Error
    with ex ->
        return Error ex
}

/// Processes terminated and undeliverable messages from NATS
type DLQProcessor(hostEnvironment: IHostEnvironment, sp: IServiceProvider) =
    inherit BackgroundService()
    
    let loggerFactory = sp.GetRequiredService<ILoggerFactory>()
    let configuration = sp.GetRequiredService<IConfiguration>()
    let mutable processor: Task = null
    
    override __.ExecuteAsync(stoppingToken) =
        let processingTask = 
            backgroundCancellableTask {
                let logger = loggerFactory.CreateLogger<DLQProcessor>()
                
                try
                    let! ct = CancellableTask.getCancellationToken()
                    
                    logger.LogInformation "Starting DLQ Processor initialization."
                    
                    let client = sp.GetRequiredService<INatsClient>()
                    do! client.ConnectAsync()
                    
                    let jsCtx : INatsJSContext = client.CreateJetStreamContext()
                    let natsConnection : NatsClient = client :?> NatsClient
                    
                    let nsConfigured = configuration |> Config.tryGetConfigValue "Namespace"
                    let ns = nsConfigured |> Option.defaultValue DLQService.Namespace
                    let nsUpper = ns.ToUpperInvariant()
                    let nsLower = ns.ToLowerInvariant()

                    // Determine environment: config value takes precedence, fallback to hosting environment
                    let envConfigured = configuration |> Config.tryGetConfigValue "Environment"
                    let env = 
                        match envConfigured with
                        | Some envStr ->
                            match envStr.ToLowerInvariant() with
                            | "development" | "dev" -> "DEV"
                            | "staging" | "stage" -> "STAGING"
                            | "production" | "prod" -> "PROD"
                            | _ -> 
                                logger.LogWarning("Unknown environment '{Env}' in configuration, falling back to hosting environment", [| box envStr |])
                                if hostEnvironment.IsDevelopment() then "DEV"
                                elif hostEnvironment.IsStaging() then "STAGING"
                                else "PROD"
                        | None ->
                            // Fallback to hosting environment
                            if hostEnvironment.IsDevelopment() then "DEV"
                            elif hostEnvironment.IsStaging() then "STAGING"
                            else "PROD"
                    
                    // Get replica count from configuration (default: 1 for compatibility with non-clustered NATS)
                    let numReplicas = 
                        configuration 
                        |> Config.tryGetConfigValue "DLQStreamReplicas"
                        |> Option.map (Config.intParseWithDefault 1)
                        |> Option.defaultValue 1
                    
                    logger.LogInformation("Using {Replicas} replica(s) for DLQ stream", [| box numReplicas |])
                    
                    // Create or update DLQ stream
                    let! dlqStreamResult = createOrUpdateDLQStream jsCtx nsUpper nsLower env numReplicas
                    let dlqStream = 
                        dlqStreamResult |> Result.defaultWith (fun err ->
                            logger.LogCritical("Failed to create or update DLQ stream: {Error}", [| box err |])
                            failwith "Failed to create or update DLQ stream."
                        )
                    
                    logger.LogInformation "✅ DLQ stream created or updated successfully."
                    
                    // Function to process advisory events and publish to DLQ
                    let processAdvisoryEvent (msg: NATS.Client.Core.NatsMsg<ReadOnlyMemory<byte>>) (failureReason: string) (logPrefix: string) (successLogFormat: string) = cancellableTask {
                        // Parse advisory event JSON
                        let jsonText = System.Text.Encoding.UTF8.GetString(msg.Data.Span)
                        use doc = JsonDocument.Parse(jsonText)
                        let root = doc.RootElement
                        
                        // Extract advisory event metadata
                        let streamName = root.TryGetProperty("stream") |> fun (found, prop) -> if found then prop.GetString() else null
                        let consumerName = root.TryGetProperty("consumer") |> fun (found, prop) -> if found then prop.GetString() else null
                        let originalSubject = root.TryGetProperty("subject") |> fun (found, prop) -> if found then prop.GetString() else null
                        let delivered = root.TryGetProperty("delivered") |> fun (found, prop) -> if found then Some (prop.GetInt32()) else None
                        
                        // Validate that the stream matches our configured namespace and environment
                        // Stream name format: {NAMESPACE}_{ENV}_*
                        if streamName <> null && streamName.StartsWith($"{nsUpper}_{env}_") then
                            logger.LogWarning("{LogPrefix}: Stream={Stream}, Consumer={Consumer}, Subject={Subject}", 
                                [| box logPrefix; box streamName; box (Option.ofObj consumerName |> Option.defaultValue "unknown"); box (Option.ofObj originalSubject |> Option.defaultValue "unknown") |])
                            
                            let dlqMsg = {
                                OriginalSubject = Option.ofObj originalSubject |> Option.defaultValue "unknown"
                                OriginalStream = streamName
                                OriginalConsumer = Option.ofObj consumerName |> Option.defaultValue "unknown"
                                OriginalData = ReadOnlyMemory<byte>.Empty // Advisory events don't contain payload
                                OriginalHeaders = msg.Headers |> Option.ofObj
                                FailureReason = failureReason
                                FailureTimestamp = DateTimeOffset.UtcNow
                                DeliveryAttempts = delivered
                                Metadata = None
                            }
                            
                            let! publishResult = publishToDLQ jsCtx nsLower env dlqMsg
                            match publishResult with
                            | Ok seq ->
                                let attemptsStr = dlqMsg.DeliveryAttempts |> Option.map string |> Option.defaultValue "unknown"
                                logger.LogInformation(
                                    successLogFormat,
                                    [| box seq; box dlqMsg.OriginalStream; box dlqMsg.OriginalConsumer; box dlqMsg.OriginalSubject; box attemptsStr |])
                            | Error err ->
                                logger.LogError("Failed to publish {FailureReason} advisory to DLQ stream: {Error}", [| box failureReason; box err |])
                        else
                            logger.LogDebug("Skipping {FailureReason} advisory event - stream doesn't match namespace/environment. Stream: {Stream}", [| box failureReason; box (Option.ofObj streamName |> Option.defaultValue "unknown") |])
                    }
                    
                    // Subscribe to NATS advisory events for terminated messages
                    // NATS automatically publishes these when services call AckTerminateAsync
                    let terminatedAdvisorySubject = "$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.>"
                    logger.LogInformation("Subscribing to terminated message advisory events: {Subject}", [| box terminatedAdvisorySubject |])
                    let terminatedAdvisorySub = natsConnection.SubscribeAsync(terminatedAdvisorySubject, cancellationToken = ct)
                    
                    // Process terminated message advisory events
                    let processTerminatedAdvisoryEvents = 
                        backgroundCancellableTask {
                            logger.LogInformation "Starting to process terminated message advisory events."
                            
                            let! ct = CancellableTask.getCancellationToken()
                            let e = terminatedAdvisorySub.GetAsyncEnumerator(ct)
                            let mutable keepGoing = true
                            
                            try
                                while not ct.IsCancellationRequested && keepGoing do
                                    let! next = e.MoveNextAsync()
                                    if not next then
                                        keepGoing <- false
                                        logger.LogInformation "No more terminated advisory events, exiting."
                                    else
                                        let msg : NATS.Client.Core.NatsMsg<ReadOnlyMemory<byte>> = e.Current
                                        if not (Object.ReferenceEquals(msg, null)) then
                                            try
                                                do! processAdvisoryEvent msg "Terminated" "Received terminated message advisory event" "Published terminated message advisory to DLQ stream at sequence: {Seq}, Stream: {Stream}, Consumer: {Consumer}, Subject: {Subject}"
                                            with ex ->
                                                logger.LogError(ex, "Error processing terminated advisory event")
                            with
                            | :? OperationCanceledException -> logger.LogInformation "Terminated advisory event processing cancelled."
                            | :? System.Net.Sockets.SocketException -> logger.LogDebug "Socket closed during shutdown while reading terminated advisory events."
                            | :? ObjectDisposedException -> logger.LogDebug "Object disposed during shutdown while reading terminated advisory events."
                            | ex -> logger.LogError(ex, "Error in terminated advisory event processing loop.")
                            
                            logger.LogInformation "Disposing terminated advisory event enumerator."
                            do! e.DisposeAsync()
                        }
                    
                    // Subscribe to NATS advisory events for max deliveries exceeded
                    // These events are published when a message exceeds MaxDeliver
                    let maxDeliveriesAdvisorySubject = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"
                    logger.LogInformation("Subscribing to max deliveries advisory events: {Subject}", [| box maxDeliveriesAdvisorySubject |])
                    let maxDeliveriesAdvisorySub = natsConnection.SubscribeAsync(maxDeliveriesAdvisorySubject, cancellationToken = ct)
                    
                    // Process max deliveries advisory events
                    let processMaxDeliveriesAdvisoryEvents = 
                        backgroundCancellableTask {
                            logger.LogInformation "Starting to process max deliveries advisory events."
                            
                            let! ct = CancellableTask.getCancellationToken()
                            let e = maxDeliveriesAdvisorySub.GetAsyncEnumerator(ct)
                            let mutable keepGoing = true
                            
                            try
                                while not ct.IsCancellationRequested && keepGoing do
                                    let! next = e.MoveNextAsync()
                                    if not next then
                                        keepGoing <- false
                                        logger.LogInformation "No more max deliveries advisory events, exiting."
                                    else
                                        let msg : NATS.Client.Core.NatsMsg<ReadOnlyMemory<byte>> = e.Current
                                        if not (Object.ReferenceEquals(msg, null)) then
                                            try
                                                do! processAdvisoryEvent msg "MaxDeliveriesExceeded" "Received max deliveries advisory event" "Published max deliveries advisory to DLQ stream at sequence: {Seq}, Stream: {Stream}, Consumer: {Consumer}, Subject: {Subject}, Attempts: {Attempts}"
                                            with ex ->
                                                logger.LogError(ex, "Error processing max deliveries advisory event")
                            with
                            | :? OperationCanceledException -> logger.LogInformation "Max deliveries advisory event processing cancelled."
                            | :? System.Net.Sockets.SocketException -> logger.LogDebug "Socket closed during shutdown while reading max deliveries advisory events."
                            | :? ObjectDisposedException -> logger.LogDebug "Object disposed during shutdown while reading max deliveries advisory events."
                            | ex -> logger.LogError(ex, "Error in max deliveries advisory event processing loop.")
                            
                            logger.LogInformation "Disposing max deliveries advisory event enumerator."
                            do! e.DisposeAsync()
                        }
                    
                    // Run all processors concurrently
                    do! Task.WhenAll([| 
                        processTerminatedAdvisoryEvents ct :> Task
                        processMaxDeliveriesAdvisoryEvents ct :> Task
                    |])
                    
                    logger.LogInformation "DLQ Processor exiting gracefully."
                    
                with
                | :? OperationCanceledException -> logger.LogInformation "DLQ Processor cancelled."
                | ex -> logger.LogError(ex, "Error in DLQ Processor.")
            }
        
        processor <- processingTask stoppingToken
        processor
    
    override __.Dispose() =
        base.Dispose()
        if not (Object.ReferenceEquals(processor, null)) then
            processor.Dispose()
            processor <- null

