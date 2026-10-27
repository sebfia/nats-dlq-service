// Tests for src/DLQService/NatsTls.fs (the NATS client certificate), copied
// from Mercator.Shared's NatsTlsTests / NatsTlsServerTests in
// sebfia/mercator-kubernetes. The repository has no test project; this script
// compiles the module from source and runs the tests with Expecto:
//
//   dotnet fsi tests/NatsTls.Tests.fsx
//
// The tests against a real server start and stop their own loopback
// nats-server (ephemeral ports, throwaway certificates; never another server)
// and run only when NATS_SERVER_BIN points to a nats-server binary:
//
//   NATS_SERVER_BIN=/path/to/nats-server dotnet fsi tests/NatsTls.Tests.fsx
//
// The NATS.Client.Core version matches paket.lock.

#r "nuget: Expecto, 11.1.1"
#r "nuget: NATS.Client.Core, 3.2.0"
#load "../src/DLQService/NatsTls.fs"

open System
open System.Diagnostics
open System.IO
open System.Net
open System.Net.Http
open System.Net.Sockets
open System.Security.Cryptography
open System.Security.Cryptography.X509Certificates
open System.Text.Json
open System.Threading.Tasks
open Expecto
open NATS.Client.Core

// ---------------------------------------------------------------------------
// Configuration (NatsTlsTests)
// ---------------------------------------------------------------------------

/// A throwaway CA, a client certificate signed by it (PKCS#8 key, as the
/// chart's cert-manager Certificates write it) and a foreign key, in a temp dir.
type Pki() =
    let dir = Directory.CreateTempSubdirectory "nats-tls-tests-"
    let write (name: string) (text: string) =
        let path = Path.Combine(dir.FullName, name)
        File.WriteAllText(path, text)
        path
    let caKey = ECDsa.Create(ECCurve.NamedCurves.nistP256)
    let ca =
        let request = CertificateRequest("CN=test-nats-client-ca", caKey, HashAlgorithmName.SHA256)
        request.CertificateExtensions.Add(X509BasicConstraintsExtension(true, false, 0, true))
        request.CertificateExtensions.Add(X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign ||| X509KeyUsageFlags.CrlSign, true))
        request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes -5.0, DateTimeOffset.UtcNow.AddDays 1.0)
    let clientKey = ECDsa.Create(ECCurve.NamedCurves.nistP256)
    let client =
        let request = CertificateRequest("CN=dlq-service", clientKey, HashAlgorithmName.SHA256)
        let clientAuth = OidCollection()
        clientAuth.Add(Oid "1.3.6.1.5.5.7.3.2") |> ignore
        request.CertificateExtensions.Add(X509EnhancedKeyUsageExtension(clientAuth, false))
        request.Create(ca, DateTimeOffset.UtcNow.AddMinutes -5.0, DateTimeOffset.UtcNow.AddHours 12.0, [| 1uy; 2uy; 3uy |])
    member val CaFile = write "ca.crt" (ca.ExportCertificatePem())
    member val CertFile = write "tls.crt" (client.ExportCertificatePem())
    member val KeyFile = write "tls.key" (clientKey.ExportPkcs8PrivateKeyPem())
    member val Sec1KeyFile = write "tls-sec1.key" (clientKey.ExportECPrivateKeyPem())
    member val ForeignKeyFile = write "foreign.key" (ECDsa.Create(ECCurve.NamedCurves.nistP256).ExportPkcs8PrivateKeyPem())
    member val EmptyFile = write "empty.crt" ""
    member _.Missing = Path.Combine(dir.FullName, "does-not-exist.crt")
    interface IDisposable with
        member _.Dispose() = try dir.Delete true with _ -> ()

let lookup (pairs: (string * string) list) = fun key -> pairs |> List.tryFind (fst >> (=) key) |> Option.map snd

let section cert key ca =
    [ if not (isNull cert) then "Nats:Tls:CertFile", cert
      if not (isNull key) then "Nats:Tls:KeyFile", key
      if not (isNull ca) then "Nats:Tls:CaFile", ca ]

let natsTlsTests =
    testList "NatsTls (mTLS client certificate for NATS)" [
        test "nothing configured: plain connection, exactly as before" {
            match NatsTls.fromSection NatsTls.Section (lookup []) with
            | Ok tls ->
                Expect.equal tls.Mode NatsTlsOpts.Default.Mode "the library default, no TLS required"
                Expect.isNull tls.CertFile "no client certificate"
                Expect.isFalse (NatsTls.presentsCertificate tls) "presents nothing"
            | Error e -> failtest e
            match NatsTls.fromSection NatsTls.Section (lookup [ "Nats:Tls:CertFile", " "; "Nats:Tls:KeyFile", "" ]) with
            | Ok tls -> Expect.isFalse (NatsTls.presentsCertificate tls) "blank values count as unset"
            | Error e -> failtest e
        }

        test "all three files: TLS is required and the client presents its certificate" {
            use pki = new Pki()
            match NatsTls.fromSection NatsTls.Section (lookup (section pki.CertFile pki.KeyFile pki.CaFile)) with
            | Ok tls ->
                Expect.equal tls.Mode TlsMode.Require "never falls back to plain text"
                Expect.equal tls.CertFile pki.CertFile "certificate"
                Expect.equal tls.KeyFile pki.KeyFile "key"
                Expect.equal tls.CaFile pki.CaFile "CA that signed the NATS server certificate"
                Expect.isFalse tls.InsecureSkipVerify "the server is verified"
                Expect.isTrue (NatsTls.presentsCertificate tls) "presents the certificate"
            | Error e -> failtest e
        }

        test "Mode: prefer for the rollout (plain until the server offers TLS), require by default, anything else fails closed" {
            use pki = new Pki()
            let withMode mode = lookup (("Nats:Tls:Mode", mode) :: section pki.CertFile pki.KeyFile pki.CaFile)
            match NatsTls.fromSection NatsTls.Section (withMode "prefer") with
            | Ok tls ->
                Expect.equal tls.Mode TlsMode.Prefer "prefer"
                Expect.isTrue (NatsTls.presentsCertificate tls) "still presents the certificate once TLS is up"
            | Error e -> failtest e
            match NatsTls.fromSection NatsTls.Section (withMode "Require") with
            | Ok tls -> Expect.equal tls.Mode TlsMode.Require "case-insensitive"
            | Error e -> failtest e
            match NatsTls.fromSection NatsTls.Section (withMode "disable") with
            | Error e -> Expect.stringContains e "Nats:Tls:Mode" "names the setting"
            | Ok _ -> failtest "an unknown mode must not weaken the connection"
            Expect.isError (NatsTls.fromSection NatsTls.Section (lookup [ "Nats:Tls:Mode", "prefer" ])) "a mode without certificate files is a partial configuration"
        }

        test "an EC key in SEC1 form loads as well" {
            use pki = new Pki()
            Expect.isOk (NatsTls.fromSection NatsTls.Section (lookup (section pki.CertFile pki.Sec1KeyFile pki.CaFile))) "EC PRIVATE KEY"
        }

        test "a partial configuration fails closed and names what is missing" {
            use pki = new Pki()
            match NatsTls.fromSection NatsTls.Section (lookup (section pki.CertFile null null)) with
            | Error e ->
                Expect.stringContains e "Nats:Tls:KeyFile" "names the missing key file setting"
                Expect.stringContains e "Nats:Tls:CaFile" "names the missing CA setting"
            | Ok _ -> failtest "a certificate without key and CA must not become a plain connection"
        }

        test "a missing or empty file fails closed and names the setting and path" {
            use pki = new Pki()
            match NatsTls.fromSection NatsTls.Section (lookup (section pki.Missing pki.KeyFile pki.CaFile)) with
            | Error e ->
                Expect.stringContains e "Nats:Tls:CertFile" "names the setting"
                Expect.stringContains e pki.Missing "names the path"
            | Ok _ -> failtest "accepted a missing certificate file"
            Expect.isError (NatsTls.fromSection NatsTls.Section (lookup (section pki.CertFile pki.KeyFile pki.EmptyFile))) "a CA file without a certificate"
        }

        test "a key that does not belong to the certificate fails closed, without echoing key material" {
            use pki = new Pki()
            match NatsTls.fromSection NatsTls.Section (lookup (section pki.CertFile pki.ForeignKeyFile pki.CaFile)) with
            | Error e ->
                Expect.stringContains e "Nats:Tls" "names the section"
                Expect.isFalse (e.Contains "PRIVATE KEY") "never prints the key"
                Expect.isFalse (e.Contains(File.ReadAllText pki.ForeignKeyFile)) "never prints the key"
            | Ok _ -> failtest "accepted a foreign key"
        }

        test "another section is read independently" {
            use pki = new Pki()
            let pairs =
                [ "Nats:Operator:Tls:CertFile", pki.CertFile
                  "Nats:Operator:Tls:KeyFile", pki.KeyFile
                  "Nats:Operator:Tls:CaFile", pki.CaFile ]
            match NatsTls.fromSection "Nats:Operator:Tls" (lookup pairs), NatsTls.fromSection NatsTls.Section (lookup pairs) with
            | Ok other, Ok main ->
                Expect.isTrue (NatsTls.presentsCertificate other) "other section"
                Expect.isFalse (NatsTls.presentsCertificate main) "main section untouched"
            | _ -> failtest "unexpected error"
        }

        test "require turns a configuration error into a startup failure (fail closed)" {
            use pki = new Pki()
            Expect.throwsT<InvalidOperationException>
                (fun () -> NatsTls.require NatsTls.Section (lookup (section pki.CertFile null null)) |> ignore)
                "never silently anonymous"
            Expect.isFalse (NatsTls.presentsCertificate (NatsTls.require NatsTls.Section (lookup []))) "unset stays plain"
        }

        test "NATS_TLS_CERT / NATS_TLS_KEY / NATS_TLS_CA" {
            use pki = new Pki()
            let env = lookup [ "NATS_TLS_CERT", pki.CertFile; "NATS_TLS_KEY", pki.KeyFile; "NATS_TLS_CA", pki.CaFile ]
            match NatsTls.fromEnvironment env with
            | Ok tls -> Expect.equal tls.Mode TlsMode.Require "required"
            | Error e -> failtest e
            match NatsTls.fromEnvironment (lookup [ "NATS_TLS_CERT", pki.CertFile ]) with
            | Error e -> Expect.stringContains e "NATS_TLS_KEY" "names the missing variable"
            | Ok _ -> failtest "partial environment accepted"
        }
    ]

// ---------------------------------------------------------------------------
// Against a scratch nats-server (NatsTlsServerTests)
// ---------------------------------------------------------------------------

let serverBin =
    Environment.GetEnvironmentVariable "NATS_SERVER_BIN" |> Option.ofObj |> Option.filter File.Exists

let freePort () =
    let listener = new TcpListener(IPAddress.Loopback, 0)
    listener.Start()
    let port = (listener.LocalEndpoint :?> IPEndPoint).Port
    listener.Stop()
    port

let eku (oid: string) =
    let oids = OidCollection()
    oids.Add(Oid oid) |> ignore
    X509EnhancedKeyUsageExtension(oids, false)

/// A CA in a temp dir that issues the server certificate and client certificates.
type ServerPki(dir: DirectoryInfo, name: string) =
    let key = ECDsa.Create(ECCurve.NamedCurves.nistP256)
    let ca =
        let request = CertificateRequest($"CN={name}", key, HashAlgorithmName.SHA256)
        request.CertificateExtensions.Add(X509BasicConstraintsExtension(true, false, 0, true))
        request.CertificateExtensions.Add(X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign ||| X509KeyUsageFlags.CrlSign, true))
        request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes -5.0, DateTimeOffset.UtcNow.AddDays 1.0)
    let path (file: string) = Path.Combine(dir.FullName, file)
    member val CaFile = (let p = path $"{name}.crt" in File.WriteAllText(p, ca.ExportCertificatePem()); p)
    /// Writes <file>.crt / <file>.key; returns their paths.
    member _.Issue(file: string, subject: string, ?server: bool) =
        let leafKey = ECDsa.Create(ECCurve.NamedCurves.nistP256)
        let request = CertificateRequest(subject, leafKey, HashAlgorithmName.SHA256)
        if defaultArg server false then
            let san = SubjectAlternativeNameBuilder()
            san.AddIpAddress IPAddress.Loopback
            san.AddDnsName "localhost"
            request.CertificateExtensions.Add(san.Build())
            request.CertificateExtensions.Add(eku "1.3.6.1.5.5.7.3.1")
        else
            request.CertificateExtensions.Add(eku "1.3.6.1.5.5.7.3.2")
        let serial = RandomNumberGenerator.GetBytes 8
        let leaf = request.Create(ca, DateTimeOffset.UtcNow.AddMinutes -5.0, DateTimeOffset.UtcNow.AddHours 12.0, serial)
        File.WriteAllText(path $"{file}.crt", leaf.ExportCertificatePem())
        File.WriteAllText(path $"{file}.key", leafKey.ExportPkcs8PrivateKeyPem())
        path $"{file}.crt", path $"{file}.key"

/// One scratch server; `connz` reports who each connection authenticated as.
type Scratch(config: Map<string, obj>, port: int, dir: DirectoryInfo) =
    let monitor = freePort ()
    let configFile = Path.Combine(dir.FullName, $"nats-{port}.json")
    let full = config |> Map.add "host" (box "127.0.0.1") |> Map.add "port" (box port) |> Map.add "http" (box $"127.0.0.1:{monitor}")
    // Relaxed escaping: the NATS config parser rejects \u escapes (e.g. for ">").
    do File.WriteAllText(configFile, JsonSerializer.Serialize(full, JsonSerializerOptions(Encoder = Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping)))
    let log = Text.StringBuilder()
    let ready = TaskCompletionSource()
    let proc =
        let info = ProcessStartInfo(serverBin.Value, $"-c \"{configFile}\"", RedirectStandardError = true, RedirectStandardOutput = true, UseShellExecute = false)
        let p = new Process(StartInfo = info)
        let onLine (e: DataReceivedEventArgs) =
            if not (isNull e.Data) then
                lock log (fun () -> log.AppendLine e.Data |> ignore)
                if e.Data.Contains "Server is ready" then ready.TrySetResult() |> ignore
        p.ErrorDataReceived.Add onLine
        p.OutputDataReceived.Add onLine
        p.Start() |> ignore
        p.BeginErrorReadLine()
        p.BeginOutputReadLine()
        p
    do
        if not (ready.Task.Wait(TimeSpan.FromSeconds 10.0)) then
            failwith $"scratch nats-server did not start:\n{log}"
    member _.Url = $"nats://127.0.0.1:{port}"
    member _.Log = lock log (fun () -> log.ToString())
    /// authorized_user per connection name, as the server reports it.
    member _.Users() = task {
        use http = new HttpClient()
        let! json = http.GetStringAsync($"http://127.0.0.1:{monitor}/connz?auth=true")
        use doc = JsonDocument.Parse json
        return
            [ for c in doc.RootElement.GetProperty("connections").EnumerateArray() do
                let prop (n: string) = match c.TryGetProperty n with | true, v -> v.GetString() | _ -> ""
                prop "name", prop "authorized_user" ]
    }
    interface IDisposable with
        member _.Dispose() =
            try proc.Kill() with _ -> ()
            proc.WaitForExit 5000 |> ignore
            proc.Dispose()

let withPki (f: DirectoryInfo -> ServerPki -> Task<unit>) = task {
    let dir = Directory.CreateTempSubdirectory "nats-tls-server-"
    try return! f dir (ServerPki(dir, "mercator-nats-ca"))
    finally try dir.Delete true with _ -> ()
}

let tlsBlock (pki: ServerPki) (serverCert: string, serverKey: string) (mode: string) =
    box (dict [ "cert_file", box serverCert; "key_file", box serverKey; "ca_file", box pki.CaFile; mode, box true; "timeout", box 5 ])

let deny (subject: string) =
    box (dict [ "publish", box (dict [ "allow", box [| ">" |]; "deny", box [| subject |] ]); "subscribe", box (dict [ "allow", box [| ">" |] ]) ])

let connect (url: string) (name: string) (tls: NatsTlsOpts) = task {
    let conn = new NatsConnection(NatsOpts(Url = url, Name = name, TlsOpts = tls, MaxReconnectRetry = 0, ConnectTimeout = TimeSpan.FromSeconds 3.0))
    try
        do! conn.ConnectAsync()
        return Ok conn
    with ex ->
        do! conn.DisposeAsync()
        return Error ex.Message
}

/// Through the service's own configuration keys (Nats__Tls__* in the chart).
let tlsOf (cert: string, key: string) (ca: string) =
    NatsTls.fromSection NatsTls.Section (lookup (section cert key ca))
    |> Result.defaultWith failwith

let natsTlsServerTests =
    match serverBin with
    | None -> testList "NatsTls against a scratch nats-server (skipped: set NATS_SERVER_BIN)" []
    | Some _ ->
        testSequenced <| testList "NatsTls against a scratch nats-server" [

            testTask "final block: the certificate is the identity; no certificate, plain text and a foreign CA are refused" {
                do! withPki (fun dir pki -> task {
                    let server = pki.Issue("server", "CN=nats", server = true)
                    let config =
                        Map [ "tls", tlsBlock pki server "verify_and_map"
                              "authorization", box (dict [ "timeout", box 10
                                                           "users", box [| box (dict [ "user", box "CN=dlq-service"; "permissions", deny "probe.dlq-service.denied" ]) |] ]) ]
                    use s = new Scratch(config, freePort (), dir)
                    let dlq = tlsOf (pki.Issue("dlq-service", "CN=dlq-service")) pki.CaFile
                    match! connect s.Url "DLQService" dlq with
                    | Ok conn ->
                        let! users = s.Users()
                        Expect.contains users ("DLQService", "CN=dlq-service") "mapped from the certificate subject"
                        do! conn.DisposeAsync()
                    | Error e -> failtest $"the dlq-service certificate was refused: {e}\n{s.Log}"
                    let! plain = connect s.Url "plain" NatsTlsOpts.Default
                    Expect.isError plain "no certificate (as before this change) is refused"
                    let! unmapped = connect s.Url "unmapped" (tlsOf (pki.Issue("stranger", "CN=stranger")) pki.CaFile)
                    Expect.isError unmapped "a certificate from our CA without a NATS user is refused"
                    let foreign = ServerPki(dir, "foreign-ca")
                    let! foreignCert = connect s.Url "foreign" (tlsOf (foreign.Issue("foreign-dlq", "CN=dlq-service")) pki.CaFile)
                    Expect.isError foreignCert "the same subject from a foreign CA is refused"
                })
            }

            testTask "rollout: a prefer client is anonymous on today's server, mapped after the switch, anonymous again after a rollback" {
                do! withPki (fun dir pki -> task {
                    let server = pki.Issue("server", "CN=nats", server = true)
                    let anonymous = box (dict [ "user", box "mercator-service"; "permissions", deny "probe.anon.denied" ])
                    let mapped = box (dict [ "user", box "CN=dlq-service"; "permissions", deny "probe.dlq-service.denied" ])
                    // today's block: no TLS, every credential-free connection is mercator-service
                    let today = Map [ "no_auth_user", box "mercator-service"; "authorization", box (dict [ "timeout", box 10; "users", box [| anonymous |] ]) ]
                    let final = Map [ "tls", tlsBlock pki server "verify_and_map"; "authorization", box (dict [ "timeout", box 10; "users", box [| mapped |] ]) ]
                    let prefer =
                        let crt, key = pki.Issue("dlq-service", "CN=dlq-service")
                        NatsTls.fromSection NatsTls.Section (lookup (("Nats:Tls:Mode", "prefer") :: section crt key pki.CaFile))
                        |> Result.defaultWith failwith
                    let identity (config: Map<string, obj>) = task {
                        use s = new Scratch(config, freePort (), dir)
                        match! connect s.Url "DLQService" prefer with
                        | Ok conn ->
                            let! users = s.Users()
                            do! conn.DisposeAsync()
                            return users |> List.tryFind (fst >> (=) "DLQService") |> Option.map snd
                        | Error e -> return failtest $"refused: {e}\n{s.Log}"
                    }
                    let! step1 = identity today
                    Expect.equal step1 (Some "mercator-service") "step 1: an image with the certificate changes nothing on today's server"
                    let! step2 = identity final
                    Expect.equal step2 (Some "CN=dlq-service") "step 2: the same client presents its certificate once the server requires TLS"
                    let! rollback = identity today
                    Expect.equal rollback (Some "mercator-service") "rollback: restoring today's block is enough"
                    // why the image must ship first: an image without the certificate cannot follow the switch
                    use s = new Scratch(final, freePort (), dir)
                    let! old = connect s.Url "old-image" NatsTlsOpts.Default
                    Expect.isError old "an old image is refused by the final block"
                })
            }

            testTask "a renewed certificate on disk is used at the next reconnect" {
                do! withPki (fun dir pki -> task {
                    let server = pki.Issue("server", "CN=nats", server = true)
                    let users = [| for cn in [ "CN=dlq-service"; "CN=dlq-service-renewed" ] -> box (dict [ "user", box cn; "permissions", deny "probe.none" ]) |]
                    let config = Map [ "tls", tlsBlock pki server "verify_and_map"; "authorization", box (dict [ "timeout", box 10; "users", box users ]) ]
                    let port = freePort ()
                    let files = pki.Issue("dlq-service", "CN=dlq-service")
                    let conn = new NatsConnection(NatsOpts(Url = $"nats://127.0.0.1:{port}", Name = "renewing", TlsOpts = tlsOf files pki.CaFile))
                    try
                        do
                            use s = new Scratch(config, port, dir)
                            conn.ConnectAsync().AsTask().Wait(TimeSpan.FromSeconds 5.0) |> ignore
                            let before = s.Users().Result
                            Expect.contains before ("renewing", "CN=dlq-service") "first certificate"
                        // cert-manager renews: new key and certificate in the same files (here with a new subject, to tell them apart)
                        let renewedCrt, renewedKey = pki.Issue("renewed", "CN=dlq-service-renewed")
                        File.Copy(renewedCrt, fst files, true)
                        File.Copy(renewedKey, snd files, true)
                        use s = new Scratch(config, port, dir)
                        let deadline = DateTime.UtcNow.AddSeconds 15.0
                        let mutable after = []
                        while DateTime.UtcNow < deadline && not (after |> List.contains ("renewing", "CN=dlq-service-renewed")) do
                            do! Task.Delay 250
                            after <- s.Users().Result
                        Expect.contains after ("renewing", "CN=dlq-service-renewed") "the reconnect presents the renewed certificate"
                    finally
                        conn.DisposeAsync().AsTask().Wait()
                })
            }
        ]

let exitCode =
    runTestsWithCLIArgs [] (fsi.CommandLineArgs |> Array.skip 1) (testList "DLQService" [ natsTlsTests; natsTlsServerTests ])

exit exitCode
