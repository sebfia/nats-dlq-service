/// OpenTelemetry instruments for dead-lettering: one span per advisory, and metrics
/// for what happened to it, how long it took and how many are still waiting.
/// Registered with the SDK in Program.fs and exported over OTLP when
/// OTEL_EXPORTER_OTLP_ENDPOINT is set (the Mercator chart sets it).
module Telemetry

open System
open System.Collections.Generic
open System.Diagnostics
open System.Diagnostics.Metrics
open System.Threading

/// Name of the ActivitySource and the Meter.
let [<Literal>] Name = "DLQService"

let activitySource = new ActivitySource(Name)
let meter = new Meter(Name)

/// What became of an advisory. The values are the `dlq.outcome` tag.
type Outcome =
    | Published
    | Filtered
    | NotFound
    | Retried
    | GivenUp
    | Unreadable
    | SkippedOwnStream
    /// Handling threw (typically the ack or nak on a lost connection); the advisory is redelivered.
    | Failed

module Outcome =
    let tag =
        function
        | Published -> "published"
        | Filtered -> "filtered"
        | NotFound -> "not_found"
        | Retried -> "retried"
        | GivenUp -> "given_up"
        | Unreadable -> "unreadable"
        | SkippedOwnStream -> "skipped_own_stream"
        | Failed -> "failed"

    /// Outcomes that mean a message is not (yet) in the DLQ because something failed.
    let isFailure =
        function
        | Retried | GivenUp | Unreadable | Failed -> true
        | Published | Filtered | NotFound | SkippedOwnStream -> false

/// Which advisory it was, from its subject.
let advisoryType (subject: string) =
    if subject.Contains(".MAX_DELIVERIES.", StringComparison.Ordinal) then "max_deliveries"
    elif subject.Contains(".MSG_TERMINATED.", StringComparison.Ordinal) then "terminated"
    else "unknown"

let private advisories =
    meter.CreateCounter<int64>(
        "dlq.advisories",
        unit = "{advisory}",
        description = "Advisories handled, by outcome, advisory type and source stream")

let private handlingDuration =
    meter.CreateHistogram<float>(
        "dlq.advisory.duration",
        unit = "s",
        description = "Time to handle one advisory, including the DLQ publish")

/// Latest backlog of the advisory consumer, refreshed by the processor; None while it is
/// unknown (before the first refresh, or after one failed), so no stale value is exported.
let private backlog : (int64 * int64) option ref = ref None

/// Advisories not yet delivered (pending) and delivered but not yet acknowledged.
let private backlogGauge =
    meter.CreateObservableGauge<int64>(
        "dlq.advisories.backlog",
        (fun () ->
            match Volatile.Read(&backlog.contents) with
            | Some (pending, ackPending) ->
                [ Measurement<int64>(pending, KeyValuePair("dlq.state", box "pending"))
                  Measurement<int64>(ackPending, KeyValuePair("dlq.state", box "ack_pending")) ]
                :> seq<_>
            | None -> Seq.empty),
        unit = "{advisory}",
        description = "Advisories waiting in the advisory stream for this consumer")

let recordBacklog (pending: int64) (ackPending: int64) =
    Volatile.Write(&backlog.contents, Some (pending, ackPending))

/// Marks the backlog unknown: the gauge reports nothing until the next successful refresh.
let clearBacklog () = Volatile.Write(&backlog.contents, None)

/// Records the outcome of one advisory on the metrics and on its span.
let record (activity: Activity) (advisoryKind: string) (sourceStream: string) (outcome: Outcome) (elapsed: TimeSpan) =
    let mutable tags = TagList()
    tags.Add("dlq.outcome", box (Outcome.tag outcome))
    tags.Add("dlq.advisory.type", box advisoryKind)
    tags.Add("dlq.source.stream", box sourceStream)
    advisories.Add(1L, &tags)
    handlingDuration.Record(elapsed.TotalSeconds, &tags)
    match Option.ofObj activity with
    | Some a ->
        a.SetTag("dlq.outcome", Outcome.tag outcome) |> ignore
        if Outcome.isFailure outcome then a.SetStatus(ActivityStatusCode.Error) |> ignore
    | None -> ()

/// Links the current span to the trace the original message was part of, when its
/// headers carry W3C trace context (NATS.Net's own tracing writes `traceparent`).
let linkOriginalTrace (traceparent: string) =
    match Option.ofObj Activity.Current, ActivityContext.TryParse(traceparent, null) with
    | Some current, (true, context) -> current.AddLink(ActivityLink(context)) |> ignore
    | _ -> ()
