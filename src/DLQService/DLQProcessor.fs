module DLQProcessor

open System
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
open Microsoft.Extensions.Configuration
open System.Net

module Task =
    let bind (f: 'a -> Task<'b>) (t: Task<'a>) : Task<'b> =
        task {
            let! x = t
            return! f x
        }
    
    let map (f: 'a -> 'b) (t: Task<'a>) : Task<'b> =
        task {
            let! x = t
            return f x
        }

module ValueTask =
    let inline bind (f: 'a -> ValueTask<'b>) (t: ValueTask<'a>) : ValueTask<'b> =
        task {
            let! x = t
            return! f x
        }
        |> ValueTask<'b>
    
    let inline map (f: 'a -> 'b) (t: ValueTask<'a>) : ValueTask<'b> =
        task {
            let! x = t
            return f x
        }
        |> ValueTask<'b>
    
    let inline apply (ft: ValueTask<'a -> 'b>) (t: ValueTask<'a>) : ValueTask<'b> =
        task {
            let! f = ft
            let! x = t
            return f x
        }
        |> ValueTask<'b>

type TerminatedAdvisory = {
    Stream: string
    Consumer: string
    StreamSeq: uint64
    ConsumerSeq: uint64
    Deliveries: int
    Reason: string option
}

module TerminatedAdvisory =
    let inline parse (data: ReadOnlyMemory<byte>) =
        use doc = JsonDocument.Parse(data)
        let root = doc.RootElement
        
        {
            Stream = root.GetProperty("stream").GetString()
            Consumer = root.GetProperty("consumer").GetString()
            StreamSeq = root.GetProperty("stream_seq").GetUInt64()
            ConsumerSeq = root.GetProperty("consumer_seq").GetUInt64()
            Deliveries = root.GetProperty("deliveries").GetInt32()
            Reason = 
                match root.TryGetProperty("reason") with
                | true, value -> Some (value.GetString())
                | false, _ -> None
        }

    let inline handleMessage (js: INatsJSContext) (dlqSubject: string) (advisory: TerminatedAdvisory) = cancellableTask {
        try
            let! ct = CancellableTask.getCancellationToken()
            // Fetch original message from stream
            let! originalMsg = 
                js.GetStreamAsync(advisory.Stream, cancellationToken = ct) 
                |> ValueTask.bind (fun stream -> 
                    stream.GetAsync(StreamMsgGetRequest(Seq = advisory.StreamSeq), cancellationToken = ct))

            match originalMsg with
            | originalMsg when originalMsg.Message = null -> 
                return Error (sprintf "Original message not found in stream %s at sequence %d" advisory.Stream advisory.StreamSeq)
            | originalMsg -> 
                let headers = NatsHeaders()
                headers.Add("X-DLQ-Original-Stream", advisory.Stream)
                headers.Add("X-DLQ-Original-Subject", originalMsg.Message.Subject)
                headers.Add("X-DLQ-Original-Time", originalMsg.Message.Time.ToString("O"))
                headers.Add("X-DLQ-Original-Seq", string advisory.StreamSeq)
                headers.Add("X-DLQ-Consumer", advisory.Consumer)
                headers.Add("X-DLQ-Deliveries", string advisory.Deliveries)
                advisory.Reason 
                |> Option.iter (fun r -> headers.Add("X-DLQ-Termination-Reason", r))
                
                // Copy original message headers - decode from base64 Hdrs field
                // NATS stores headers in base64-encoded format, we decode and parse them
                if originalMsg.Message <> null && not (String.IsNullOrEmpty(originalMsg.Message.Hdrs)) then
                    let hdrsBytes = Convert.FromBase64String(originalMsg.Message.Hdrs)
                    let hdrsText = System.Text.Encoding.UTF8.GetString(hdrsBytes)
                    // Parse NATS header format: each line is "Key: Value"
                    // Skip first line (NATS/1.0) and parse remaining headers
                    let headerLines = hdrsText.Split([|'\r'; '\n'|], StringSplitOptions.RemoveEmptyEntries)
                    headerLines
                    |> Array.skip 1 // Skip "NATS/1.0" line
                    |> Array.iter (fun line ->
                        match line.IndexOf(':') with
                        | -1 -> ()
                        | colonIdx ->
                            let key = line.Substring(0, colonIdx).Trim()
                            let value = line.Substring(colonIdx + 1).Trim()
                            headers.Add($"X-DLQ-{key}", value))
                
                // Publish to DLQ with the headers we've built
                let! ack = js.PublishAsync(dlqSubject, originalMsg.Message.Data, headers = headers, cancellationToken = ct)
                return Ok ack
        with ex ->
            return Error (sprintf "Failed to handle terminated message: %s" ex.Message)
    }

/// Creates or updates the DLQ stream
let inline createOrUpdateDLQStream (js: INatsJSContext) (ns: string) (env: string) (numReplicas: int) = cancellableTask {
    try
        let! ct = CancellableTask.getCancellationToken()
        
        let streamName = $"{ns.ToUpperInvariant()}_{env}_DLQ"
        let subject = $"{ns.ToLowerInvariant()}.{env.ToLowerInvariant()}.dlq.>"
        
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

/// Constructs the DLQ subject for a given terminated advisory
let inline subjectFor (ns: string) (env: string) (ta: TerminatedAdvisory) =
    $"{ns.ToLowerInvariant()}.{env.ToLowerInvariant()}.dlq.{ta.Stream.ToLowerInvariant()}.{ta.Consumer.ToLowerInvariant()}"

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
                    let! ct = Async.CancellationToken
                    
                    logger.LogInformation "Starting DLQ Processor initialization."
                    
                    let client = sp.GetRequiredService<INatsClient>()
                    do! client.ConnectAsync()
                    
                    let jsCtx : INatsJSContext = client.CreateJetStreamContext()
                    let natsConnection : NatsClient = client :?> NatsClient
                    
                    let nsConfigured = configuration |> Config.tryGetConfigValue "Namespace"
                    let ns = nsConfigured |> Option.defaultValue DLQService.Namespace

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
                                logger.LogWarning $"Unknown environment '{envStr}' in configuration, falling back to hosting environment"
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
                    let! dlqStreamResult = createOrUpdateDLQStream jsCtx ns env numReplicas
                    let _ =
                        dlqStreamResult |> Result.defaultWith (fun err ->
                            logger.LogCritical $"Failed to create or update DLQ stream: {err}"
                            failwith "Failed to create or update DLQ stream."
                        )
                    
                    logger.LogInformation "✅ DLQ stream created or updated successfully."
                    
                    // Subscribe to NATS advisory events for terminated messages
                    // NATS automatically publishes these when services call AckTerminateAsync
                    let terminatedAdvisorySubject = "$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.>"
                    // Subscribe to NATS advisory events for max deliveries exceeded
                    // These events are published when a message exceeds MaxDeliver
                    let maxDeliveriesAdvisorySubject = "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"

                    
                    let subjectFor' ta = subjectFor ns env ta
                    
                    // Process terminated message advisory events
                    let processTerminatedAdvisoryEvents = 
                        let p =
                            asyncEx {
                                logger.LogInformation "Starting to process terminated message advisory events."
                                
                                let! ct = Async.CancellationToken

                                let terminatedAdvisorySub = natsConnection.SubscribeAsync(terminatedAdvisorySubject, cancellationToken = ct)
                                logger.LogInformation($"Subscribing to terminated message advisory events: {terminatedAdvisorySubject}")

                                for msg : NatsMsg<ReadOnlyMemory<byte>> in terminatedAdvisorySub do
                                    try
                                        let advisory = TerminatedAdvisory.parse msg.Data
                                        let subject = subjectFor' advisory
                                        let! response = TerminatedAdvisory.handleMessage jsCtx subject advisory ct
                                        match response with
                                        | Ok ack when ack.Error <> null ->
                                            logger.LogInformation($"Processed terminated advisory message for stream '{advisory.Stream}' and consumer '{advisory.Consumer}' at stream sequence {advisory.StreamSeq} with seq {ack.Seq}") 
                                        | Ok ack ->
                                            logger.LogError($"Processed terminated advisory message for stream '{advisory.Stream}' and consumer '{advisory.Consumer}' at stream sequence {advisory.StreamSeq}, but publish to DLQ returned error: {ack.Error}")
                                        | Error err ->
                                            logger.LogError($"Failed to process terminated advisory message for Stream={advisory.Stream}, Consumer={advisory.Consumer}, Seq={advisory.StreamSeq}: {err}")
                                    with
                                    | :? OperationCanceledException -> logger.LogInformation "Terminated advisory event processing cancelled."
                                    | :? Sockets.SocketException -> logger.LogDebug "Socket closed during shutdown while reading terminated advisory events."
                                    | :? ObjectDisposedException -> logger.LogDebug "Object disposed during shutdown while reading terminated advisory events."
                                    | ex -> logger.LogError(ex, "Error in terminated advisory event processing loop.")
                                
                                logger.LogInformation "Terminated advisory subscription is ending."
                            }
                        Async.StartAsTask(p, cancellationToken = ct) :> Task
                    
                    // Process max deliveries advisory events
                    let processMaxDeliveriesAdvisoryEvents = 
                        let p =
                            asyncEx {
                                logger.LogInformation "Starting to process max deliveries advisory events."

                                logger.LogInformation($"Subscribing to max deliveries advisory events: {maxDeliveriesAdvisorySubject}")
                                let maxDeliveriesAdvisorySub = natsConnection.SubscribeAsync(maxDeliveriesAdvisorySubject, cancellationToken = ct)
                                
                                let! ct = Async.CancellationToken
                                
                                for msg : NatsMsg<ReadOnlyMemory<byte>> in maxDeliveriesAdvisorySub do
                                    try
                                        let advisory = TerminatedAdvisory.parse msg.Data
                                        let subject = subjectFor' advisory
                                        let! response = TerminatedAdvisory.handleMessage jsCtx subject advisory ct
                                        match response with
                                        | Ok ack when ack.Error <> null ->
                                            logger.LogInformation($"Processed terminated advisory message for stream '{advisory.Stream}' and consumer '{advisory.Consumer}' at stream sequence {advisory.StreamSeq} with seq {ack.Seq}") 
                                        | Ok ack ->
                                            logger.LogError($"Processed terminated advisory message for stream '{advisory.Stream}' and consumer '{advisory.Consumer}' at stream sequence {advisory.StreamSeq}, but publish to DLQ returned error: {ack.Error}")
                                        | Error err ->
                                            logger.LogError($"Failed to process terminated advisory message for Stream={advisory.Stream}, Consumer={advisory.Consumer}, Seq={advisory.StreamSeq}: {err}")
                                    with
                                    | :? OperationCanceledException -> logger.LogInformation "Max deliveries advisory event processing cancelled."
                                    | :? Sockets.SocketException -> logger.LogDebug "Socket closed during shutdown while reading max deliveries advisory events."
                                    | :? ObjectDisposedException -> logger.LogDebug "Object disposed during shutdown while reading max deliveries advisory events."
                                    | ex -> logger.LogError(ex, "Error in max deliveries advisory event processing loop.")
                                
                                logger.LogInformation "Disposing max deliveries advisory event enumerator."
                            }
                        Async.StartAsTask(p, cancellationToken = ct) :> Task
                        
                    
                    // Run all processors concurrently
                    do! Task.WhenAll([| 
                        processTerminatedAdvisoryEvents
                        processMaxDeliveriesAdvisoryEvents
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
