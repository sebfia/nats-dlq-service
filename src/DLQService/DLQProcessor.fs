module DLQProcessor

open System
open System.Threading
open System.Threading.Tasks
open System.Threading.Channels
open System.Text.Json
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

type AdvisoryMessageResult =
    | PublishedToDLQ of PubAckResponse
    | FilteredOut of subject: string * expectedPrefix: string
    | MessageNotFound of stream: string * sequence: uint64
    | PublishError of ack: PubAckResponse
    | ProcessingError of error: string

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

    let inline handleMessage (js: INatsJSContext) (dlqSubject: string) (expectedSubjectPrefix: string) (advisory: TerminatedAdvisory) (ct: CancellationToken) = task {
        try
            // Fetch original message from stream
            let! originalMsg = 
                js.GetStreamAsync(advisory.Stream, cancellationToken = ct) 
                |> ValueTask.bind (fun stream -> 
                    stream.GetAsync(StreamMsgGetRequest(Seq = advisory.StreamSeq), cancellationToken = ct))

            match originalMsg with
            | originalMsg when originalMsg.Message = null -> 
                return MessageNotFound (advisory.Stream, advisory.StreamSeq)
            | originalMsg ->
                // Filter: only process messages from the configured namespace.env pattern
                match originalMsg.Message.Subject.StartsWith(expectedSubjectPrefix, StringComparison.OrdinalIgnoreCase) with
                | false ->
                    return FilteredOut (originalMsg.Message.Subject, expectedSubjectPrefix)
                | true ->
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
                    
                    // Check if publish was successful
                    return 
                        if ack.Error = null then 
                            PublishedToDLQ ack
                        else 
                            PublishError ack
        with ex ->
            return ProcessingError (sprintf "Failed to handle terminated message: %s" ex.Message)
    }

/// DLQ Stream configuration
type DLQStreamConfig = {
    NumReplicas: int
    Retention: StreamConfigRetention
    Storage: StreamConfigStorage
    NoAck: bool
    Compression: StreamConfigCompression
    MaxAge: TimeSpan
    Discard: StreamConfigDiscard
    AllowDirect: bool
    DuplicateWindow: TimeSpan
    MaxMsgs: int64
    MaxBytes: int64
    MaxMsgSize: int
    MaxConsumers: int
    AllowUpdateStream: bool
}

module DLQStreamConfig =
    let fromConfiguration (configuration: IConfiguration) : DLQStreamConfig =
        let getConfigValue key = configuration |> Config.tryGetConfigValue key
        
        {
            NumReplicas = 
                getConfigValue "DLQStream:NumReplicas"
                |> Option.bind Config.tryParseInt
                |> Option.defaultValue 1
            
            Retention = 
                getConfigValue "DLQStream:Retention"
                |> Option.bind (fun v -> 
                    match v.ToLowerInvariant() with
                    | "limits" -> Some StreamConfigRetention.Limits
                    | "interest" -> Some StreamConfigRetention.Interest
                    | "workqueue" -> Some StreamConfigRetention.Workqueue
                    | _ -> None)
                |> Option.defaultValue StreamConfigRetention.Limits
            
            Storage = 
                getConfigValue "DLQStream:Storage"
                |> Option.bind (fun v -> 
                    match v.ToLowerInvariant() with
                    | "file" -> Some StreamConfigStorage.File
                    | "memory" -> Some StreamConfigStorage.Memory
                    | _ -> None)
                |> Option.defaultValue StreamConfigStorage.File
            
            NoAck = 
                getConfigValue "DLQStream:NoAck"
                |> Option.bind Config.tryParseBool
                |> Option.defaultValue false
            
            Compression = 
                getConfigValue "DLQStream:Compression"
                |> Option.bind (fun v -> 
                    match v.ToLowerInvariant() with
                    | "none" -> Some StreamConfigCompression.None
                    | "s2" -> Some StreamConfigCompression.S2
                    | _ -> None)
                |> Option.defaultValue StreamConfigCompression.None
            
            MaxAge = 
                getConfigValue "DLQStream:MaxAgeDays"
                |> Option.bind Config.tryParseFloat
                |> Option.map TimeSpan.FromDays
                |> Option.defaultValue TimeSpan.Zero
            
            Discard = 
                getConfigValue "DLQStream:Discard"
                |> Option.bind (fun v -> 
                    match v.ToLowerInvariant() with
                    | "old" -> Some StreamConfigDiscard.Old
                    | "new" -> Some StreamConfigDiscard.New
                    | _ -> None)
                |> Option.defaultValue StreamConfigDiscard.Old
            
            AllowDirect = 
                getConfigValue "DLQStream:AllowDirect"
                |> Option.bind Config.tryParseBool
                |> Option.defaultValue true
            
            DuplicateWindow = 
                getConfigValue "DLQStream:DuplicateWindowMinutes"
                |> Option.bind Config.tryParseFloat
                |> Option.map TimeSpan.FromMinutes
                |> Option.defaultValue (TimeSpan.FromMinutes 2.0)
            
            MaxMsgs = 
                getConfigValue "DLQStream:MaxMsgs"
                |> Option.bind Config.tryParseInt64
                |> Option.defaultValue -1L
            
            MaxBytes = 
                getConfigValue "DLQStream:MaxBytes"
                |> Option.bind Config.tryParseInt64
                |> Option.defaultValue -1L
            
            MaxMsgSize = 
                getConfigValue "DLQStream:MaxMsgSize"
                |> Option.bind Config.tryParseInt
                |> Option.defaultValue -1
            
            MaxConsumers = 
                getConfigValue "DLQStream:MaxConsumers"
                |> Option.bind Config.tryParseInt
                |> Option.defaultValue -1
            
            AllowUpdateStream = 
                getConfigValue "DLQStream:AllowUpdateStream"
                |> Option.bind Config.tryParseBool
                |> Option.defaultValue true
        }

/// Creates or updates the DLQ stream
let inline createOrUpdateDLQStream (js: INatsJSContext) (ns: string) (env: string) (streamConfig: DLQStreamConfig) (logger: ILogger) (ct: CancellationToken) = task {
    try
        
        let streamName = $"{ns.ToUpperInvariant()}_{env}_DLQ"
        let subject = $"{ns.ToLowerInvariant()}.{env.ToLowerInvariant()}.dlq.>"
        
        // Check if stream exists and if updates are allowed
        let! existingStream = 
            task {
                try
                    let! stream = js.GetStreamAsync(streamName, cancellationToken = ct)
                    return Some stream
                with
                | _ -> return None
            }
        
        match existingStream, streamConfig.AllowUpdateStream with
        | Some stream, false ->
            logger.LogInformation($"DLQ stream '{streamName}' already exists and AllowUpdateStream is false. Skipping update.")
            return Ok stream
        | _ ->
            let config = StreamConfig(
                name = streamName,
                subjects = [| subject |],
                Retention = streamConfig.Retention,
                Storage = streamConfig.Storage,
                NoAck = streamConfig.NoAck,
                Compression = streamConfig.Compression,
                NumReplicas = streamConfig.NumReplicas,
                MaxAge = streamConfig.MaxAge,
                Discard = streamConfig.Discard,
                AllowDirect = streamConfig.AllowDirect,
                DuplicateWindow = streamConfig.DuplicateWindow,
                MaxMsgs = streamConfig.MaxMsgs,
                MaxBytes = streamConfig.MaxBytes,
                MaxMsgSize = streamConfig.MaxMsgSize,
                MaxConsumers = streamConfig.MaxConsumers
            )
            
            let! stream = js.CreateOrUpdateStreamAsync(config, ct)
            return Ok stream
    with ex -> 
        return Error (sprintf "Failed to create or update DLQ stream: %s" ex.Message)
}

let inline processAdvisoryEvents 
    (natsConnection: NatsClient) 
    (jsCtx: INatsJSContext) 
    (advisorySubject: string) 
    (subjectFor': TerminatedAdvisory -> string) 
    (expectedSubjectPrefix: string) 
    (logger: ILogger)
    (ct: CancellationToken) = 
        task {
            logger.LogInformation $"Starting to process advisory events for subject: {advisorySubject}"

            let channelOptions = BoundedChannelOptions(1024)
            channelOptions.SingleWriter <- true
            channelOptions.SingleReader <- false
            channelOptions.FullMode <- BoundedChannelFullMode.Wait

            let channel = Channel.CreateBounded<TerminatedAdvisory>(channelOptions)
            let reader = channel.Reader
            let writer = channel.Writer

            let advisorySub = natsConnection.SubscribeAsync(advisorySubject, cancellationToken = ct)
            logger.LogDebug $"Subscribing to advisory events: {advisorySubject}"

            // Producer: read from NATS subscription and push into the channel
            let producer : Task =
                task {
                    let enumerator = advisorySub.GetAsyncEnumerator(ct)
                    try
                        let mutable keepReading = true
                        while keepReading do
                            let! hasItem = enumerator.MoveNextAsync()
                            if hasItem then
                                let msg : NatsMsg<ReadOnlyMemory<byte>> = enumerator.Current
                                let advisory = TerminatedAdvisory.parse msg.Data
                                do! writer.WriteAsync(advisory, ct)
                            else
                                keepReading <- false
                    with
                    | :? OperationCanceledException ->
                        logger.LogInformation $"Advisory event processing cancelled for subject {advisorySubject}."
                    | :? Sockets.SocketException ->
                        logger.LogDebug $"Socket closed while reading advisory events for subject {advisorySubject}."
                    | :? ObjectDisposedException ->
                        logger.LogDebug $"Object disposed during shutdown while reading advisory events for subject {advisorySubject}."
                    | ex ->
                        logger.LogError(ex, $"Error while reading advisory events for subject {advisorySubject}.")
                    do! enumerator.DisposeAsync()
                    writer.TryComplete() |> ignore
                }

            let workerCount =
                Environment.ProcessorCount
                |> max 1
                |> min 8

            let worker (workerId: int) : System.Threading.Tasks.Task =
                task {
                    try
                        let mutable running = true
                        while running do
                            let! hasItem = reader.WaitToReadAsync(ct)
                            if hasItem then
                                let mutable draining = true
                                while draining do
                                    let mutable item = Unchecked.defaultof<TerminatedAdvisory>
                                    if reader.TryRead(&item) then
                                        try
                                            let subject = subjectFor' item
                                            let! response =
                                                TerminatedAdvisory.handleMessage jsCtx subject expectedSubjectPrefix item ct
                                            match response with
                                            | PublishedToDLQ ack ->
                                                logger.LogInformation $"[Worker {workerId}] ✅ Published message to DLQ from stream '{item.Stream}', consumer '{item.Consumer}', sequence {item.StreamSeq} → DLQ sequence {ack.Seq}"
                                            | FilteredOut (msgSubject, expectedPrefix) ->
                                                logger.LogDebug $"[Worker {workerId}] ⏩ Filtered out message from stream '{item.Stream}', consumer '{item.Consumer}', sequence {item.StreamSeq}: subject '{msgSubject}' does not match expected prefix '{expectedPrefix}'"
                                            | MessageNotFound (stream, seq) ->
                                                logger.LogWarning $"[Worker {workerId}] ⚠️ Original message not found in stream '{stream}' at sequence {seq}"
                                            | PublishError ack ->
                                                logger.LogError $"[Worker {workerId}] ❌ Failed to publish to DLQ for stream '{item.Stream}', consumer '{item.Consumer}', sequence {item.StreamSeq}: {ack.Error}"
                                            | ProcessingError err ->
                                                logger.LogError $"[Worker {workerId}] ❌ Processing error for stream '{item.Stream}', consumer '{item.Consumer}', sequence {item.StreamSeq}: {err}"
                                        with
                                        | :? OperationCanceledException ->
                                            raise (OperationCanceledException())
                                        | ex ->
                                            logger.LogError(ex, $"[Worker {workerId}] Exception while processing advisory message for stream '{item.Stream}', consumer '{item.Consumer}', sequence {item.StreamSeq}")
                                    else
                                        draining <- false
                            else
                                running <- false
                    with
                    | :? OperationCanceledException ->
                        logger.LogInformation $"Advisory processing worker {workerId} cancelled for subject {advisorySubject}."
                }

            let workers : System.Threading.Tasks.Task[] = [| for i in 1 .. workerCount -> worker i |]
            let allTasks : System.Threading.Tasks.Task[] = Array.append [| producer |] workers

            do! Task.WhenAll(allTasks)
        }

/// Constructs the DLQ subject for a given terminated advisory
let inline subjectFor (ns: string) (env: string) (ta: TerminatedAdvisory) =
    $"{ns.ToLowerInvariant()}.{env.ToLowerInvariant()}.dlq.{ta.Stream.ToLowerInvariant()}.{ta.Consumer.ToLowerInvariant()}"

/// Processes terminated and undeliverable messages from NATS
type DLQProcessor(hostEnvironment: IHostEnvironment, sp: IServiceProvider) =
    inherit BackgroundService()
    
    let loggerFactory = sp.GetRequiredService<ILoggerFactory>()
    let configuration = sp.GetRequiredService<IConfiguration>()
    
    override __.ExecuteAsync(stoppingToken) =
        task {
            let logger = loggerFactory.CreateLogger<DLQProcessor>()
            
            try
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
                
                // Load DLQ stream configuration from appsettings
                let dlqStreamConfig = DLQStreamConfig.fromConfiguration configuration
                
                logger.LogInformation("DLQ Stream Configuration: Replicas={Replicas}, Retention={Retention}, Storage={Storage}, Compression={Compression}, MaxAgeDays={MaxAgeDays}, AllowUpdateStream={AllowUpdateStream}", 
                    [| box dlqStreamConfig.NumReplicas
                       box dlqStreamConfig.Retention
                       box dlqStreamConfig.Storage
                       box dlqStreamConfig.Compression
                       box dlqStreamConfig.MaxAge.TotalDays
                       box dlqStreamConfig.AllowUpdateStream |])
                
                // Create or update DLQ stream
                let! dlqStreamResult = createOrUpdateDLQStream jsCtx ns env dlqStreamConfig logger stoppingToken
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
                
                // Build the expected subject prefix once for filtering
                let expectedSubjectPrefix = $"{ns.ToLowerInvariant()}.{env.ToLowerInvariant()}."
                
                // Process terminated message advisory events
                let processTerminatedAdvisoryEvents : Task = 
                    processAdvisoryEvents natsConnection jsCtx terminatedAdvisorySubject subjectFor' expectedSubjectPrefix logger stoppingToken :> Task
                
                // Process max deliveries advisory events
                let processMaxDeliveriesAdvisoryEvents : Task = 
                    processAdvisoryEvents natsConnection jsCtx maxDeliveriesAdvisorySubject subjectFor' expectedSubjectPrefix logger stoppingToken :> Task
                    
                
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
