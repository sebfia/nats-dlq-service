/// The NATS client certificate (mTLS). The Mercator NATS server maps the
/// certificate's subject to a NATS user (`verify_and_map`), so a client
/// presenting one sends no user, password or token. cert-manager issues the
/// certificate; the Mercator chart mounts it and sets `Nats__Tls__CertFile` /
/// `KeyFile` / `CaFile` (and `Mode` during the rollout).
///
/// An explicitly configured certificate that cannot be used never silently
/// becomes a plain, anonymous connection. Nothing configured leaves the
/// connection exactly as before (local runs without TLS). No reflection: safe
/// for the trimmed single-file image.
///
/// Mirrors `Mercator.Shared.NatsTls` in sebfia/mercator-kubernetes
/// (src/shared/Mercator.Shared/Common/NatsTls.fs); keep the two in step.
/// Left out: mesh's second section (`Nats:Operator:Tls`), which only mesh uses.
[<RequireQualifiedAccess>]
module NatsTls

open System
open System.IO
open System.Security.Cryptography
open System.Security.Cryptography.X509Certificates
open NATS.Client.Core

/// The configuration section: `Nats:Tls:CertFile`, `Nats:Tls:KeyFile`,
/// `Nats:Tls:CaFile`, `Nats:Tls:Mode` (env `Nats__Tls__CertFile` …).
[<Literal>]
let Section = "Nats:Tls"

/// The three settings as (certificate, key, CA) names, e.g. configuration keys
/// or environment variables. The CA is the one that signed the NATS SERVER's
/// certificate: the client verifies the server with it.
/// `Mode` (optional) is `require` (default) or `prefer`: the rollout's first
/// step, where the client presents its certificate as soon as the server
/// offers TLS and stays plain until then.
type Names = { Cert: string; Key: string; Ca: string; Mode: string option }

let sectionNames (section: string) =
    { Cert = $"{section}:CertFile"; Key = $"{section}:KeyFile"; Ca = $"{section}:CaFile"; Mode = Some $"{section}:Mode" }

/// `NATS_TLS_CERT` / `NATS_TLS_KEY` / `NATS_TLS_CA`, as the Mercator CLI and
/// operator scripts name them (always `require`).
let environmentNames = { Cert = "NATS_TLS_CERT"; Key = "NATS_TLS_KEY"; Ca = "NATS_TLS_CA"; Mode = None }

let private nonBlank = Option.filter (String.IsNullOrWhiteSpace >> not)

/// Checks that the files can be used, and names the setting and path when
/// not. Never includes file contents: the key file holds a private key.
let private check (names: Names) (mode: TlsMode) (cert: string, key: string, ca: string) : Result<NatsTlsOpts, string> =
    let missing =
        [ names.Cert, cert; names.Key, key; names.Ca, ca ]
        |> List.filter (fun (_, path) -> not (File.Exists path))
    match missing with
    | (name, path) :: _ -> Error $"{name} = '{path}': file not found (a configured NATS client certificate is never ignored)"
    | [] ->
        let pair =
            try
                use certificate = X509Certificate2.CreateFromPemFile(cert, key)
                if certificate.HasPrivateKey then Ok()
                else Error $"{names.Key} = '{key}' holds no private key for {names.Cert}"
            with
            | :? CryptographicException
            | :? ArgumentException ->
                Error $"{names.Cert} = '{cert}' and {names.Key} = '{key}' are not a matching PEM certificate and private key"
        let authority () =
            try
                let bundle = X509Certificate2Collection()
                bundle.ImportFromPemFile ca
                if bundle.Count > 0 then Ok()
                else Error $"{names.Ca} = '{ca}' holds no PEM certificate"
            with
            | :? CryptographicException ->
                Error $"{names.Ca} = '{ca}' holds no readable PEM certificate"
        pair
        |> Result.bind authority
        |> Result.map (fun () -> NatsTlsOpts(CertFile = cert, KeyFile = key, CaFile = ca, Mode = mode))

/// `NatsTlsOpts.Default` (unchanged, plain) when none of the three is set;
/// TLS with the client certificate when all three are set and usable; an error
/// naming the setting otherwise.
let fromNames (names: Names) (get: string -> string option) : Result<NatsTlsOpts, string> =
    let value name = get name |> nonBlank |> Option.map (fun v -> v.Trim())
    let mode = names.Mode |> Option.bind (fun name -> value name |> Option.map (fun v -> name, v))
    let tlsMode =
        match mode with
        | None -> Ok TlsMode.Require
        | Some (_, v) when v.Equals("require", StringComparison.OrdinalIgnoreCase) -> Ok TlsMode.Require
        | Some (_, v) when v.Equals("prefer", StringComparison.OrdinalIgnoreCase) -> Ok TlsMode.Prefer
        | Some (name, v) -> Error $"{name} = '{v}': use require (default) or prefer"
    match value names.Cert, value names.Key, value names.Ca with
    | None, None, None when mode.IsNone -> Ok NatsTlsOpts.Default
    | Some cert, Some key, Some ca -> tlsMode |> Result.bind (fun m -> check names m (cert, key, ca))
    | cert, key, ca ->
        let absent =
            [ if cert.IsNone then names.Cert
              if key.IsNone then names.Key
              if ca.IsNone then names.Ca ]
        Error $"""NATS client certificate partly configured: set {String.Join(", ", absent)} as well (or none of {names.Cert}, {names.Key}, {names.Ca})"""

/// From a configuration section, given a configuration lookup
/// (`fun key -> config.[key] |> Option.ofObj`).
let fromSection (section: string) (get: string -> string option) = fromNames (sectionNames section) get

/// `NATS_TLS_CERT` / `NATS_TLS_KEY` / `NATS_TLS_CA`.
let fromEnvironment (getEnv: string -> string option) = fromNames environmentNames getEnv

/// Fail closed at service startup: a configuration error is thrown, never
/// turned into a plain connection.
let require (section: string) (get: string -> string option) : NatsTlsOpts =
    match fromSection section get with
    | Ok tls -> tls
    | Error reason -> invalidOp reason

/// Whether these options present a client certificate once the connection is
/// TLS (the server then knows the client by it, and a username must not be
/// sent alongside).
let presentsCertificate (tls: NatsTlsOpts) =
    (tls.Mode = TlsMode.Require || tls.Mode = TlsMode.Prefer) && not (String.IsNullOrEmpty tls.CertFile)
