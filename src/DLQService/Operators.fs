namespace FSharp.Sebfia

[<RequireQualifiedAccess>]
module Railroad =

    let inline joinAsync (a: Result<'a,'err>) (joiner: 'a -> 'b -> 'u) (b: Result<'b,'err>) = async {
        match a with
        | Ok a' -> 
            match b with
            | Ok b' -> 
                let result = joiner a' b'
                return Ok result
            | Error e -> return Error e
        | Error e -> return Error e
    }

    let inline join (a: Result<'a,'err>) (joiner: 'a -> 'b -> 'u) (b: Result<'b,'err>) =
        match a with
        | Ok a' -> 
            match b with
            | Ok b' -> 
                let result = joiner a' b'
                Ok result
            | Error e -> Error e
        | Error e -> Error e

    let inline mapError (map: 'errA -> 'errB) (a: Result<'a,'errA>) =
        match a with
        | Error x -> map x |> Error
        | Ok y -> Ok y

    let inline failOnError (f: 'err -> exn) (a: Result<'a,'err>) =
        match a with
        | Ok a -> a
        | Error e -> f e |> raise

    let inline mapOption (fNone: unit -> 'err) = function | Some a -> Ok a | _ -> fNone() |> Error
 
module Operators =
    let inline (>|>) (v: 'a option) (f: 'a -> 'b option) = match v with | Some x -> f x | _ -> None

    let inline (>=>) (v: Result<'a,'err>) (f: 'a -> Result<'b,'err>) = 
        match v with 
        | Ok x -> 
            match box x with
            | :? System.IDisposable as d ->
                let r = f x
                d.Dispose()
                r
            | _ -> f x

        | Error e -> Error e

    let inline (>|=>) (f: 'a -> Result<'b,'err>) (errIfNone: 'err) (v: 'a option) = match v with | Some x -> f x | _ -> Error errIfNone

    let inline (|>=) a f = Railroad.mapOption f a

    let inline (>==>) (v: Async<Result<'a,'err>>) (f: 'a -> Async<Result<'b,'err>>) = async { match! v with | Ok x -> return! f x | Error e -> return Error e }

    let inline (>=>=>) (v: Result<'a, 'err>) (f: 'a -> Async<Result<'b,'err>>) = async { match v with | Ok x -> return! f x | Error e -> return Error e}

    let inline (>||==>) (v: Async<'a option>) (f: 'a -> Async<Result<'b, 'err>>) (errIfNone: 'err) = async { match! v with | Some x -> return! f x | _ -> return Error errIfNone }

    let inline (=--=) (v: Async<Result<'a,'err>>) (f: 'a -> Async<'b>) = async { 
        match! v with 
        | Ok x -> 
            let! y = f x
            return Ok y
        | Error e -> return Error e
    }

    let inline (=-=) (v: Result<'a,'err>) (f: 'a -> 'b) = 
        match v with 
        | Ok x -> 
            match box x with
            | :? System.IDisposable as d ->
                let r = f x
                d.Dispose()
                r |> Ok
            | _ -> f x |> Ok
        | Error e -> Error e

