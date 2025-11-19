module Config
open System
open System.IO
open System.Text.RegularExpressions
open Microsoft.Extensions.Configuration
open System.Text.RegularExpressions


let inline tryGetEnvironmentVariable name =
    match Environment.GetEnvironmentVariable name with
    | x when String.IsNullOrEmpty x -> None
    | s -> Some s
let inline tryGetConfigValue key (c: IConfiguration) =
    if isNull c then None else
    match c.[key] with
    | s when (String.IsNullOrWhiteSpace s |> not) -> Some (Regex.Replace(s, @"\s+", ""))
    | _ -> None
let inline tryGetSectionValue section key (c: IConfiguration) =
    if isNull c then None else
    try 
        c.GetSection(sprintf "%s:%s" section key).Value |> Some
    with _ -> None
let inline tryGetSectionArray section key (c: IConfiguration) =
    if isNull c then None else
    try
        c.GetSection(sprintf "%s:%s" section key).GetChildren() |> Seq.map (fun x -> x.Value) |> Seq.toArray |> Some
    with _ -> None
let inline tryGetSectionMap<'key,'value> section key mapKeyName mapValueName (c: IConfiguration) =
    if isNull c then None else
    try
        c.GetSection(sprintf "%s:%s" section key).GetChildren() |> Seq.map (fun x -> x.GetValue<'key>(mapKeyName),x.GetValue<'value>(mapValueName)) |> Seq.toArray |> Some
    with _ -> None
let inline enumToList<'a> = (Enum.GetValues(typeof<'a>) :?> ('a [])) |> Array.toList

let inline private mapTryFindReplacement (m: Text.RegularExpressions.Match) map : string option =
    let s = sprintf "%A" m
    let key = (s.Replace("{", "")).Replace("}", "")
    Map.tryFind key map
let inline replacer map  =
    fun (m: Text.RegularExpressions.Match) -> mapTryFindReplacement m map |> function | None -> "" | Some x -> x

let inline expandTemplate t =
    let specialFolders =  enumToList<Environment.SpecialFolder> |> List.map (fun v -> (sprintf "%A" v),Environment.GetFolderPath v)
    let regex = Regex(@"\{\w+\}", RegexOptions.Compiled ||| RegexOptions.Singleline ||| RegexOptions.CultureInvariant)
    fun expansions ->
        let map = List.concat [specialFolders; expansions] |> Map.ofList
        regex.Replace(t, (replacer map))

let inline adaptOSDirectorySeparator (s: string) =
        match Runtime.InteropServices.RuntimeInformation.IsOSPlatform(Runtime.InteropServices.OSPlatform.Windows) with
        | true -> s.Replace('/', Path.DirectorySeparatorChar)
        | _ -> s.Replace('\\', Path.DirectorySeparatorChar)

let inline tryParseInt (str: string) =
    match Int32.TryParse str with
    | (true, i) -> Some i
    | _ -> None

let inline tryParseInt64 (str: string) =
    match Int64.TryParse str with
    | (true, i) -> Some i
    | _ -> None

let inline tryParseBool (str: string) =
    match Boolean.TryParse str with
    | (true, b) -> Some b
    | _ -> None

let inline tryParseFloat (str: string) =
    match Double.TryParse str with
    | (true, f) -> Some f
    | _ -> None

let inline isRunningInDocker() =
    File.Exists("/.dockerenv") ||
    (File.Exists("/proc/1/cgroup") &&
     (try
         let content = File.ReadAllText("/proc/1/cgroup")
         content.Contains("docker") || content.Contains("kubepods")
      with _ -> false))

// "/Users/sebastian/Documents/blobs" |> expandTemplate
// "{Docs}/blobs" |> expandEnvironmentFolderIfNecessary
// "{MyDocuments}\\blobs" |> expandTemplate
// "{MyDocuments}/blobs/readModel/volatilities" |> expandTemplate

