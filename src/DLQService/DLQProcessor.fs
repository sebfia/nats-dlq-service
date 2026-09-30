module DLQProcessor

open System
open System.Diagnostics
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
open Mercator.HealthChecks

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
    /// Absent from `max_deliver` advisories; only `terminated` ones carry it.
    ConsumerSeq: uint64 option
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
            ConsumerSeq =
                match root.TryGetProperty("consumer_seq") with
                | true, value -> Some (value.GetUInt64())
                | false, _ -> None
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
                                // Join the original trace, if its producer propagated one
                                if key.Equals("traceparent", StringComparison.OrdinalIgnoreCase) then
                                    Telemetry.linkOriginalTrace value
                                headers.Add($"X-DLQ-{key}", value))
                    
                    // Publish to DLQ with the headers we've built. The message id makes a
                    // redelivered advisory (handled, but its ack lost) a duplicate the DLQ
                    // stream drops within its duplicate window, not a second DLQ entry.
                    let msgId = $"{advisory.Stream}:{advisory.Consumer}:{advisory.StreamSeq}"
                    let! ack = js.PublishAsync(dlqSubject, originalMsg.Message.Data, headers = headers, opts = NatsJSPubOpts(MsgId = msgId), cancellationToken = ct)
                    
                    // Check if publish was successful
                    return 
                        if ack.Error = null then 
                            PublishedToDLQ ack
                        else 
                            PublishError ack
        with
        // The stream or the message is gone (deleted, or aged out before the advisory
        // was handled): nothing to dead-letter, and retrying cannot change that.
        | :? NatsJSApiException as ex when ex.Error.Code = 404 ->
            return MessageNotFound (advisory.Stream, advisory.StreamSeq)
        | ex ->
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

/// Where advisories wait until they are handled. Advisories are published on core
/// NATS subjects: a subscriber that is down, restarting or too slow never sees them.
/// A stream captures them instead, and each environment's durable consumer works
/// through them at its own pace.
type AdvisoryStreamConfig = {
    /// One stream per account: advisories are account-wide, and NATS rejects two
    /// streams whose subjects overlap. Every environment's consumer reads it.
    Name: string
    NumReplicas: int
    Storage: StreamConfigStorage
    /// How long advisories are kept. Limits retention: each consumer tracks its own
    /// position, and nothing is discarded because no consumer exists yet (first
    /// start) or any more (a deleted or retired consumer).
    MaxAge: TimeSpan
}

module AdvisoryStreamConfig =
    let subjects = [|
        // Published when a consumer calls AckTerminateAsync
        "$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.>"
        // Published when a message exceeds its consumer's MaxDeliver
        "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"
    |]

    let fromConfiguration (dlq: DLQStreamConfig) (configuration: IConfiguration) : AdvisoryStreamConfig =
        let getConfigValue key = configuration |> Config.tryGetConfigValue key
        {
            Name = getConfigValue "AdvisoryStream:Name" |> Option.defaultValue "DLQ_ADVISORIES"
            NumReplicas =
                getConfigValue "AdvisoryStream:NumReplicas"
                |> Option.bind Config.tryParseInt
                |> Option.defaultValue dlq.NumReplicas
            Storage = dlq.Storage
            MaxAge =
                getConfigValue "AdvisoryStream:MaxAgeDays"
                |> Option.bind Config.tryParseFloat
                |> Option.map TimeSpan.FromDays
                |> Option.defaultValue (TimeSpan.FromDays 7.0)
        }

/// Creates the advisory stream when it is missing. An existing one is used as it is:
/// it is shared by every environment, so no single service owns its settings.
let inline ensureAdvisoryStream (js: INatsJSContext) (config: AdvisoryStreamConfig) (logger: ILogger) (ct: CancellationToken) = task {
    try
        let! existing =
            task {
                try
                    let! stream = js.GetStreamAsync(config.Name, cancellationToken = ct)
                    return Some stream
                with
                | :? NatsJSApiException as ex when ex.Error.Code = 404 -> return None
            }
        match existing with
        | Some stream ->
            logger.LogInformation("Advisory stream '{Stream}' exists; using it as it is.", config.Name)
            return Ok stream
        | None ->
            let streamConfig = StreamConfig(
                name = config.Name,
                subjects = AdvisoryStreamConfig.subjects,
                Retention = StreamConfigRetention.Limits,
                Storage = config.Storage,
                NumReplicas = config.NumReplicas,
                MaxAge = config.MaxAge,
                Discard = StreamConfigDiscard.Old)
            let! stream = js.CreateStreamAsync(streamConfig, ct)
            logger.LogInformation("Created advisory stream '{Stream}' (replicas {Replicas}, max age {MaxAgeDays} days).", config.Name, config.NumReplicas, config.MaxAge.TotalDays)
            return Ok stream
    with ex ->
        return Error (sprintf "Failed to create advisory stream '%s': %s" config.Name ex.Message)
}

/// Attempts per advisory before it is given up (and logged as lost).
let [<Literal>] AdvisoryMaxDeliver = 10

/// Delay before a failed advisory is retried, growing with each attempt.
let inline retryDelay (deliveries: int64) =
    TimeSpan.FromSeconds(min 60.0 (2.0 ** float (max 0L (deliveries - 1L))))

/// What to do with an advisory once it has been handled.
type AdvisoryOutcome =
    | Done
    | Retry of reason: string

module AdvisoryOutcome =
    let ofResult =
        function
        | PublishedToDLQ _ | FilteredOut _ | MessageNotFound _ -> Done
        | PublishError ack -> Retry (sprintf "DLQ publish failed: %O" ack.Error)
        | ProcessingError err -> Retry err

/// Works through the advisory stream with the environment's durable consumer. Only a
/// handled advisory is acknowledged; a failed one is retried after a delay, so an
/// advisory is lost only after AdvisoryMaxDeliver failures, never to a burst, a
/// restart or a slow worker. Replicas of this service share the consumer.
let inline consumeAdvisories
    (jsCtx: INatsJSContext)
    (advisoryStream: string)
    (consumerName: string)
    (dlqStream: string)
    (subjectFor': TerminatedAdvisory -> string)
    (expectedSubjectPrefix: string)
    (logger: ILogger)
    (ct: CancellationToken) =
        task {
            let consumerConfig =
                ConsumerConfig(
                    consumerName,
                    AckPolicy = ConsumerConfigAckPolicy.Explicit,
                    DeliverPolicy = ConsumerConfigDeliverPolicy.All,
                    AckWait = TimeSpan.FromSeconds 30.0,
                    MaxDeliver = int64 AdvisoryMaxDeliver,
                    MaxAckPending = 1000L)
            let! consumer = jsCtx.CreateOrUpdateConsumerAsync(advisoryStream, consumerConfig, ct)
            logger.LogInformation("Consuming advisories from '{Stream}' with durable consumer '{Consumer}'.", advisoryStream, consumerName)

            let handle (workerId: int) (msg: INatsJSMsg<ReadOnlyMemory<byte>>) = task {
                // One span per advisory; the DLQ publish and the JetStream lookups run inside it.
                use activity = Telemetry.activitySource.StartActivity("dlq advisory", ActivityKind.Consumer)
                let started = Stopwatch.GetTimestamp()
                let advisoryKind = Telemetry.advisoryType msg.Subject
                let deliveries = msg.Metadata |> Option.ofNullable |> Option.map (fun m -> int64 m.NumDelivered) |> Option.defaultValue 1L
                let! sourceStream, outcome = task {
                    match (try Ok (TerminatedAdvisory.parse msg.Data) with ex -> Error ex.Message) with
                    | Error err ->
                        logger.LogError("[Worker {Worker}] Unreadable advisory on {Subject}, skipped: {Error}", workerId, msg.Subject, err)
                        do! msg.AckAsync(cancellationToken = ct)
                        return "unknown", Telemetry.Unreadable
                    | Ok advisory when advisory.Stream = dlqStream || advisory.Stream = advisoryStream ->
                        // A DLQ consumer, or this service's own consumer, gave up on a
                        // message: dead-lettering it again would loop.
                        logger.LogWarning("[Worker {Worker}] Skipped advisory about stream '{Stream}' (sequence {Seq}): it is the DLQ's own stream.", workerId, advisory.Stream, advisory.StreamSeq)
                        do! msg.AckAsync(cancellationToken = ct)
                        return advisory.Stream, Telemetry.SkippedOwnStream
                    | Ok advisory ->
                        match Option.ofObj activity with
                        | Some a ->
                            a.SetTag("dlq.advisory.type", advisoryKind)
                             .SetTag("dlq.source.stream", advisory.Stream)
                             .SetTag("dlq.source.consumer", advisory.Consumer)
                             .SetTag("dlq.source.sequence", int64 advisory.StreamSeq)
                             .SetTag("dlq.deliveries", deliveries) |> ignore
                        | None -> ()
                        let! result = TerminatedAdvisory.handleMessage jsCtx (subjectFor' advisory) expectedSubjectPrefix advisory ct
                        match result with
                        | PublishedToDLQ ack ->
                            logger.LogInformation $"[Worker {workerId}] ✅ Published message to DLQ from stream '{advisory.Stream}', consumer '{advisory.Consumer}', sequence {advisory.StreamSeq} → DLQ sequence {ack.Seq}"
                        | FilteredOut (msgSubject, expectedPrefix) ->
                            logger.LogDebug $"[Worker {workerId}] ⏩ Filtered out message from stream '{advisory.Stream}', consumer '{advisory.Consumer}', sequence {advisory.StreamSeq}: subject '{msgSubject}' does not match expected prefix '{expectedPrefix}'"
                        | MessageNotFound (stream, seq) ->
                            logger.LogWarning $"[Worker {workerId}] ⚠️ Original message not found in stream '{stream}' at sequence {seq}"
                        | PublishError _ | ProcessingError _ -> ()

                        match AdvisoryOutcome.ofResult result with
                        | Done ->
                            do! msg.AckAsync(cancellationToken = ct)
                            let outcome =
                                match result with
                                | PublishedToDLQ _ -> Telemetry.Published
                                | FilteredOut _ -> Telemetry.Filtered
                                | _ -> Telemetry.NotFound
                            return advisory.Stream, outcome
                        | Retry reason when deliveries >= int64 AdvisoryMaxDeliver ->
                            logger.LogError("[Worker {Worker}] ❌ Giving up on stream '{Stream}', consumer '{Consumer}', sequence {Seq} after {Deliveries} attempts; it is not in the DLQ: {Reason}", workerId, advisory.Stream, advisory.Consumer, advisory.StreamSeq, deliveries, reason)
                            do! msg.AckAsync(cancellationToken = ct)
                            return advisory.Stream, Telemetry.GivenUp
                        | Retry reason ->
                            let delay = retryDelay deliveries
                            logger.LogWarning("[Worker {Worker}] Attempt {Deliveries} failed for stream '{Stream}', consumer '{Consumer}', sequence {Seq}; retrying in {Delay}: {Reason}", workerId, deliveries, advisory.Stream, advisory.Consumer, advisory.StreamSeq, delay, reason)
                            do! msg.NakAsync(delay = delay, cancellationToken = ct)
                            return advisory.Stream, Telemetry.Retried
                }
                Telemetry.record activity advisoryKind sourceStream outcome (Stopwatch.GetElapsedTime started)
            }

            /// One pass over the consumer: returns whether any advisory was handled, and
            /// the exception that ended it, if any.
            let consumeOnce (workerId: int) (restart: bool) = task {
                // A restart re-declares the consumer, so one deleted by hand comes back.
                let! consumer =
                    if restart then jsCtx.CreateOrUpdateConsumerAsync(advisoryStream, consumerConfig, ct)
                    else ValueTask<INatsJSConsumer>(consumer)
                let messages = consumer.ConsumeAsync<ReadOnlyMemory<byte>>(opts = NatsJSConsumeOpts(MaxMsgs = 256), cancellationToken = ct).GetAsyncEnumerator(ct)
                let mutable handledAny = false
                let! failure = task {
                    try
                        while! messages.MoveNextAsync() do
                            try
                                do! handle workerId messages.Current
                                handledAny <- true
                            with
                            | :? OperationCanceledException -> raise (OperationCanceledException())
                            | ex -> logger.LogError(ex, "[Worker {Worker}] Exception while handling advisory {Subject}; it will be redelivered.", workerId, messages.Current.Subject)
                        return None
                    with ex -> return Some ex }
                try do! messages.DisposeAsync() with _ -> ()
                return handledAny, failure }

            /// Keeps a worker consuming until shutdown. A consume loop that ends or fails
            /// (a lost connection, a deleted consumer) is logged and restarted with a
            /// growing delay, so capacity never drains away one silent worker at a time.
            let worker (workerId: int) : Task =
                task {
                    let mutable failures = 0L
                    while not ct.IsCancellationRequested do
                        let! handledAny, failure =
                            task {
                                try return! consumeOnce workerId (failures > 0L)
                                with ex -> return false, Some ex }
                        if handledAny then failures <- 0L
                        match failure with
                        | _ when ct.IsCancellationRequested -> ()
                        | Some (:? OperationCanceledException) -> ()
                        | outcome ->
                            failures <- failures + 1L
                            let delay = retryDelay failures
                            match outcome with
                            | Some ex -> logger.LogError(ex, "[Worker {Worker}] Consume loop failed ({Failures} in a row); restarting in {Delay}.", workerId, failures, delay)
                            | None -> logger.LogWarning("[Worker {Worker}] Consume loop ended; restarting in {Delay}.", workerId, delay)
                            try do! Task.Delay(delay, ct) with :? OperationCanceledException -> ()
                    logger.LogInformation("Advisory worker {Worker} stopped.", workerId)
                }

            /// Keeps the dlq.advisories.backlog gauge current (consumer info, every 30 s).
            let refreshBacklog : Task =
                task {
                    while not ct.IsCancellationRequested do
                        try
                            let! current = jsCtx.GetConsumerAsync(advisoryStream, consumerName, ct)
                            Telemetry.recordBacklog (int64 current.Info.NumPending) current.Info.NumAckPending
                        with
                        | :? OperationCanceledException -> ()
                        | ex -> logger.LogDebug(ex, "Could not read the advisory consumer's backlog.")
                        try do! Task.Delay(TimeSpan.FromSeconds 30.0, ct) with :? OperationCanceledException -> ()
                }

            let workerCount = Environment.ProcessorCount |> max 1 |> min 8
            do! Task.WhenAll [| yield refreshBacklog; for i in 1 .. workerCount -> worker i |]
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
                
                let advisoryConfig = AdvisoryStreamConfig.fromConfiguration dlqStreamConfig configuration
                let! advisoryStreamResult = ensureAdvisoryStream jsCtx advisoryConfig logger stoppingToken
                let _ =
                    advisoryStreamResult |> Result.defaultWith (fun err ->
                        logger.LogCritical $"Failed to set up the advisory stream: {err}"
                        failwith "Failed to set up the advisory stream."
                    )

                let subjectFor' ta = subjectFor ns env ta

                // Build the expected subject prefix once for filtering
                let expectedSubjectPrefix = $"{ns.ToLowerInvariant()}.{env.ToLowerInvariant()}."

                do! consumeAdvisories
                        jsCtx
                        advisoryConfig.Name
                        $"DLQService_{ns.ToUpperInvariant()}_{env}"
                        $"{ns.ToUpperInvariant()}_{env}_DLQ"
                        subjectFor'
                        expectedSubjectPrefix
                        logger
                        stoppingToken

                logger.LogInformation "DLQ Processor exiting gracefully."
                
            with
            | :? OperationCanceledException -> logger.LogInformation "DLQ Processor cancelled."
            | ex ->
                // Nothing is dead-lettered from here on: fail /alive so the Pod is
                // restarted instead of staying green while idle.
                let healthStore = sp.GetRequiredService<ServiceHealthStore>()
                logger.LogNonResolvableError(healthStore, "DLQProcessor", "DLQ Processor stopped", ex)
        }
