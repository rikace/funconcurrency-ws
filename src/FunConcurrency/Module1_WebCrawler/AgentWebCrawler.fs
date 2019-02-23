module FunConcurrency.AgentWebCrawler

#if INTERACTIVE
#load "../Common/Helpers.fs"
#load "../Module3_Asynchronous/Async.fs"
#r "../../../packages/HtmlAgilityPack/lib/netstandard2.0/HtmlAgilityPack.dll"
#endif

open System
open System.Threading
open System.Net
open System.IO
open HtmlAgilityPack
open System.Text.RegularExpressions
open FunConcurrency


// Extracts links from HTML.
let extractLinks html =
    let pattern1 = "(?i)href\\s*=\\s*(\"|\')/?((?!#.*|/\B|mailto:|location\.|javascript:)[^\"\']+)(\"|\')"
    let pattern2 = "(?i)^https?"

    let links =
        [
            for x in Regex(pattern1).Matches(html) do
                yield x.Groups.[2].Value
        ] |> List.filter (fun x -> Regex(pattern2).IsMatch(x))
    links

// Download Html/Page content
let downloadContent (url : string) = async {
    try
        let req = WebRequest.Create(url) :?> HttpWebRequest
        req.UserAgent <- "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)"
        req.Timeout <- 5000
        use! resp = req.GetResponseAsync() |> Async.AwaitTask
        let content = resp.ContentType
        let isHtml = Regex("html").IsMatch(content)
        match isHtml with
        | true -> use stream = resp.GetResponseStream()
                  use reader = new StreamReader(stream)
                  let! html = reader.ReadToEndAsync() |> Async.AwaitTask
                  return Some html
        | false -> return None
    with
    | _ -> return None
}

module ParallelWebCrawler =

    type Msg<'a, 'b> =
    | Item of 'a
    | Mailbox of Agent<Msg<'a, 'b>>

    let cts = new CancellationTokenSource()

    let [<Literal>] parallelism = 4 // can be any arbitrary value

    let httpRgx = new Regex(@"^(http|https|www)://.*$")

    let sites = [
       "http://cnn.com/";          "http://bbc.com/";
       "http://www.yahoo.com";     "http://www.amazon.com"
       "http://news.yahoo.com";    "http://www.microsoft.com";
       "http://www.google.com";    "http://www.netflix.com";
       "http://www.bing.com";      "http://www.microsoft.com";
       "http://www.yahoo.com";     "http://www.amazon.com"
       "http://news.yahoo.com";    "http://www.microsoft.com"; ]

    // Step (1) create an Agent that prints the messages.
    //    this is important in parallel computations that print some output
    //    to keep the console in a readable state
    let printerAgent =
        Agent<Msg<string, string>>.Start((fun inbox -> async {
          while true do
            let! msg = inbox.Receive()
            match msg with
            | Item(t) -> printfn "%s" t
            | Mailbox(agent) -> failwith "no implemented"}), cancellationToken = cts.Token)


    // Test
    printerAgent.Post (Item "Hello from printerAgent!!")

    // Step (2)
    //     create a "parallelAgent" worker based on the MailboxPorcerssor.
    //     the idea is to have an Agent that handles, computes and distributes the messages
    //     in a Round-Robin fashion between a set of (intern and pre-instantiated) Agent-children
    //
    //     This is important in the case of async computaions, so you can achieve great throughput
    //     If already completed the "Agent Pipeline" lab, then feel free to use the "parallelAgent" already created

    let parallelAgent n f =
        // MISSING CODE HERE
        let agents = Array.init n (fun _ ->
            Agent<Msg<'a, 'b>>.Start(f, cancellationToken = cts.Token))

        let token = cts.Token

        let agent = new Agent<Msg<'a, 'b>>((fun inbox ->
            let rec loop index = async {
                let! msg = inbox.Receive()
                match msg with
                | Msg.Item(item) ->
                    agents.[index].Post (Item item)
                    return! loop ((index + 1) % n)
                | Mailbox(agent) ->
                    agents |> Seq.iter(fun a -> a.Post (Mailbox agent))
                    return! loop ((index + 1) % n)
            }
            loop 0), cancellationToken = token)

        token.Register(fun () -> agents |> Seq.iter(fun agent -> (agent :> IDisposable).Dispose())) |> ignore
        agent.Start()
        agent

    // Step (3) complete the "Item(url)" case

    // Step (3) complete the "Item(url)" case
    let fetchContetAgent (limit : int option) =
        parallelAgent parallelism (fun inbox ->
            let rec loop (urls : Set<string>) (agents : Agent<_> list) = async {
                let! msg = inbox.Receive()

                match msg with
                | Item(url) ->
                    // check if the content of the "url" has been alredy
                    // downloaded.
                    // if not then
                    //     downloaded the content (use the function "downloadContent")
                    //     and print (using the "printerAgent") a message that the "content of url %s hes been downloaded"
                    //
                    //    IMPORTANT: the content is passed (broadcast) as message to all the agents subscribed to this agent.
                    //               the registration is done using the "Mailbox(agent)" message/case.
                    //               The list of agent subscribed is kept as state of the agent loop (agents : Agent<_> list)
                    // else
                    //     nothing
                    // verify if the limit of the Urls downloaded is reached, and stop the process accordingly
                    // (keep in mind that the "limit" is an option type (if None then the process is limiteless)

                    if urls |> Set.contains url |> not then
                        let! content = downloadContent url
                        content |> Option.iter(fun c ->
                            for agent in agents do
                                agent.Post (Item(c)))
                        let urls' = (urls |> Set.add url)
                        match limit with
                        | Some l when urls' |> Seq.length >= l -> cts.Cancel()
                        | _ -> return! loop urls' agents
                    else return! loop urls agents
                | Mailbox(agent) -> return! loop urls (agent::agents)
            }
            loop Set.empty [])


    // Testing
    let testFetchContetAgent () =
        let agent = fetchContetAgent (Some 5)
        agent.Post (Mailbox(printerAgent))
        for site in sites do agent.Post (Item site)

    testFetchContetAgent()


    // Step (4)  create a broadcast agent, which simply broadcasts (forward)
    //           the messages received to all the agents subscribed
    //     Bonus:    would be nice to have a filter in place to select
    //               which agent receives which message (no required)
    let broadcastAgent () =
        parallelAgent parallelism (fun inbox ->
            let rec loop (agents : Agent<Msg<_,_>> list) = async {
                let! msg = inbox.Receive()
                match msg with
                // the content is passed (broadcast) as message to all the agents subscribed to this agent.
                // the registration is done using the "Mailbox(agent)" message/case.
                // The list of agent subscribed is kept as state of the agent loop (agents : Agent<_> list)
                | Item(item) ->
                    for agent in agents do
                        agent.Post(Item(item))
                    return! loop agents

                | Mailbox(agent) -> return! loop (agent::agents)
            }
            loop [])


    // Testing
    let testBroadcastAgent1() =
        let brcast = broadcastAgent()
        brcast.Post (Mailbox(printerAgent))
        brcast.Post (Mailbox(printerAgent))
        for site in sites do brcast.Post (Item site)

    testBroadcastAgent1()

    // Step (5)  Implement a "link" agent parser.
    //           - the message "Mailbox(agent)" subscribes agent(s)
    //           - the message "Item(url)" to delivers an url to process
    //
    //           implement an agent that extract the "href" tags from a web page
    //           and send the reference (href) to the Agent subscribed  as link
    let linksParserAgent () =
        parallelAgent parallelism (fun inbox ->
            let rec loop (agents : Agent<_> list) = async {
                let! msg = inbox.Receive()
                match msg with
                | Item(html) ->

                    let doc = new HtmlDocument()
                    doc.LoadHtml(html)

                    let links =
                        doc.DocumentNode.Descendants("a")
                        |> Seq.choose(fun n ->
                            if n.Attributes.Contains("href") then
                                n.GetAttributeValue("href", "") |> Some
                            else None)
                        |> Seq.filter(fun url -> httpRgx.IsMatch(url)) // NOTE, IS THIS CORRECT ??

                    // broadcast the links extracted to all the "agents" subscribed
                    // (use the "Item" case to send the "link" extracted
                    // Missing code

                    return! loop agents
                // Add the missing case to register/subscribe Agents
                // | ...
            }
            loop [])


    // imageParserAgent implementation will be pushed on github after the
    // implementation of the "linksParserAgent"

    let comparison = StringComparison.InvariantCultureIgnoreCase
    let linkFilter =
        fun (link : string) ->
            link.IndexOf(".aspx", comparison) <> -1 ||
            link.IndexOf(".php", comparison) <> -1 ||
            link.IndexOf(".htm", comparison) <> -1 ||
            link.IndexOf(".html", comparison) <> -1

    let imageSideEffet (f: string -> byte[] -> Async<unit>) =
        parallelAgent parallelism (fun inbox ->
            let rec loop () = async {
                let! msg = inbox.Receive()
                match msg with
                | Item(url) ->
                    if linkFilter url then
                        let client = new WebClient()
                        let! buffer = client.DownloadDataTaskAsync(url) |> Async.AwaitTask
                        do! f url buffer
                | _ -> failwith "no implemented"
                return! loop ()
            }
            loop ())

    // Step (6)
    // complete the "side effect" function.
    // For example, you could just print the image name downloaded and/or save it to the file-system
    let saveImageAgent : Agent<Msg<string, unit>> =
        imageSideEffet (fun url buffer -> async {
                let fileName = Path.GetFileName(url)
                let name = @"Images\" + fileName

                // Missing code
                ()
            })

    type WebCrawler (?limit) as this =
        let fetchContetAgent = fetchContetAgent limit
        let contentBroadcaster = broadcastAgent ()
        let linkBroadcaster = broadcastAgent ()
        let linksParserAgent = linksParserAgent ()

        //   remove comment below
        //   let imageParserAgent = imageParserAgent ()

        // Step (6)
        // Register/subscribe the agents to compose and run the Web-Crawler
        do
            fetchContetAgent.Post     (Mailbox(contentBroadcaster))

            // MISSING CODE for registration

        member __.Submit(url : string) = fetchContetAgent.Post(Item(url))

        member __.Dispose() = cts.Cancel()

        interface IDisposable with
            member x.Dispose() = this.Dispose()

// BONUS
// what happen if an error is thrown ??
// we could use the Agent build in functionality for Error
// propagation to do something, like logging or recovering from errors

type MailboxProcessor<'T> with
    member inline this.withSupervisor (supervisor: Agent<exn>, transform) =
        this.Error.Add(fun error -> supervisor.Post(transform(error))); this

    member this.withSupervisor (supervisor: Agent<exn>) =
        this.Error.Add(supervisor.Post); this

