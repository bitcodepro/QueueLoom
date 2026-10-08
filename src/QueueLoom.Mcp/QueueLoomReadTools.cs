using QueueLoom.Core.Profiles;
using System.ComponentModel;
using ModelContextProtocol;
using ModelContextProtocol.Server;
using System.Globalization;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Mcp;

/// <summary>
/// Read-only tools. They never settle or send messages, so they run without approval, except a read of a live queue
/// on a service without peek (see <see cref="ConfirmLiveQueueReadAsync"/>).
/// </summary>
[McpServerToolType]
public sealed class QueueLoomReadTools(McpWorkspaceSession session, McpServerSettings settings, IServiceProvider services)
{
    private IDeadLetterHistoryStore? History => services.GetService(typeof(IDeadLetterHistoryStore)) as IDeadLetterHistoryStore;

    /// <summary>
    /// SQS, Pub/Sub and RabbitMQ have no peek: a read receives the messages and releases them, which counts as a
    /// delivery (the SQS receive count, Pub/Sub delivery attempts, a quorum queue's delivery count). On a live queue a
    /// redrive or dead-letter policy can then move (or, without a dead-letter target, discard) messages that were only
    /// looked at, so a person approves that read
    /// first, as for a change. Dead-letter queues, Azure Service Bus (real peek) and Kafka (offsets) read freely.
    /// </summary>
    private async Task ConfirmLiveQueueReadAsync(McpServer server, ServiceBusProfile profile, string entity,
        ServiceBusSubQueue queue, int count, CancellationToken cancellationToken)
    {
        if (queue != ServiceBusSubQueue.Active || profile.Provider is MessagingProvider.AzureServiceBus or MessagingProvider.Kafka)
        {
            return;
        }
        if (services.GetService(typeof(IOperationApprover)) is not IOperationApprover approver)
        {
            throw new McpException("Reading live messages here needs approval, and no approval is available in this QueueLoom.");
        }
        // The client names itself and the model names the entity: one line each, so neither can add lines of its own.
        var client = server.ClientInfo is { } info ? ApprovalText.OneLine($"{info.Name} {info.Version}".Trim()) : "an MCP client";
        var decision = await approver.RequestAsync(
            new ApprovalRequest(
                "Read live messages",
                profile.Name,
                profile.Environment == EnvironmentKind.Production,
                $"Requested by: {client}\nEnvironment: {profile.Name} ({profile.EnvironmentDisplayName})\n" +
                $"{profile.Provider.DisplayName()}: {profile.EndpointDisplay}\n\n" +
                $"Up to {count:N0} live message(s) of '{ApprovalText.OneLine(entity.Trim())}' will be received and released at once. " +
                $"{profile.Provider.DisplayName()} counts each read as a delivery, so a redrive or dead-letter policy can move " +
                "these messages to a dead-letter queue, or, where none is set, discard them for good" +
                (profile.Provider == MessagingProvider.RabbitMq
                    ? " (a RabbitMQ quorum queue drops a message past its delivery limit, 20 by default since RabbitMQ 4.0, " +
                      "unless it has a dead-letter exchange)"
                    : string.Empty) +
                ". QueueLoom itself deletes and sends nothing."),
            server,
            cancellationToken).ConfigureAwait(false);
        if (!decision.Approved)
        {
            session.Record("Info", "Read live messages not approved", decision.Reason, profile);
            throw new McpException($"Not read: {decision.Reason}");
        }
    }

    private const string EnvironmentDescription =
        "Saved environment name (see list_environments). Optional when only one environment is saved.";

    private const string QueryDescription =
        "Plain text is found in IDs, subject, reason, properties and body, ignoring case. '/regex/' is a regular expression " +
        "('/…/i' ignores case). '$.order.status == \'failed\'' checks a field of the JSON body (also gzip or base64): ==, !=, >, >=, <, <=, " +
        "=~ /regex/, or the path alone for 'has the field'; join conditions with and / or; [0] picks a list item, [*] any.";

    [McpServerTool(Name = "list_environments", Title = "List environments", ReadOnly = true, Idempotent = true, OpenWorld = false)]
    [Description("Lists the environments saved in QueueLoom (name, kind such as Production, service such as Amazon SQS / SNS, " +
        "namespace/region/project, authentication, access mode).")]
    public Task<IReadOnlyList<EnvironmentInfo>> ListEnvironmentsAsync(CancellationToken cancellationToken) =>
        McpGuard.RunAsync<IReadOnlyList<EnvironmentInfo>>(async () =>
            (await session.ListProfilesAsync(cancellationToken).ConfigureAwait(false)).Select(McpMapping.ToInfo).ToArray());

    [McpServerTool(Name = "get_entities", Title = "List queues, topics and subscriptions", ReadOnly = true, Idempotent = true)]
    [Description("Lists queues, topics and subscriptions with active, dead-letter, transfer dead-letter and scheduled message counts. " +
                   "Subscriptions are named 'topic/subscription'. Use typedEntity to disambiguate queues and exchanges with the same name.")]
    public Task<TopologyInfo> GetEntitiesAsync(
        [Description(EnvironmentDescription)] string? environment = null,
        [Description("Optional case-insensitive text that entity names must contain.")] string? filter = null,
        CancellationToken cancellationToken = default) =>
        McpGuard.RunAsync(async () =>
        {
            var profile = await session.ResolveProfileAsync(environment, cancellationToken).ConfigureAwait(false);
            var topology = await session.ReadAsync(profile,
                    (workspace, token) => workspace.GetTopologyAsync(forceRefresh: true, token), cancellationToken)
                .ConfigureAwait(false);
            string Kind(string name) => char.ToUpperInvariant(name[0]) + name[1..];
            var entities = topology.Queues.Select(queue => McpMapping.ToInfo(queue.Reference, queue.Runtime, queue.Status) with { Kind = Kind(topology.QueueKindName) })
                .Concat(topology.Topics.SelectMany(topic =>
                    new[] { McpMapping.ToInfo(topic.Reference, topic.Runtime, topic.Status) with { Kind = Kind(topology.TopicKindName) } }
                        .Concat(topic.Subscriptions.Select(subscription =>
                            McpMapping.ToInfo(subscription.Reference, subscription.Runtime, subscription.Status)))))
                  .Select(entity => entity with { TypedEntity = $"{entity.Kind.ToLowerInvariant()}:{entity.Entity}" })
                  .Where(entity => string.IsNullOrWhiteSpace(filter) ||
                                 entity.Entity.Contains(filter.Trim(), StringComparison.OrdinalIgnoreCase))
                .ToArray();
            return new TopologyInfo(profile.Name, topology.FetchedAt, topology.UsesSampledCounts, entities);
        });

    [McpServerTool(Name = "scan_dead_letters", Title = "Scan dead-letter queues", ReadOnly = true, Idempotent = true)]
    [Description("Counts dead-lettered messages in every queue and subscription of the environment and returns the non-empty ones.")]
    public Task<DeadLetterScanInfo> ScanDeadLettersAsync(
        [Description(EnvironmentDescription)] string? environment = null,
        CancellationToken cancellationToken = default) =>
        McpGuard.RunAsync(async () =>
        {
            var profile = await session.ResolveProfileAsync(environment, cancellationToken).ConfigureAwait(false);
            var snapshot = await session.ReadAsync(profile,
                    (workspace, token) => workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All, token), cancellationToken)
                .ConfigureAwait(false);
            await RecordHistoryAsync(snapshot, profile.Name, cancellationToken).ConfigureAwait(false);
            return new DeadLetterScanInfo(
                profile.Name,
                snapshot.CapturedAt,
                snapshot.TotalCount,
                snapshot.Entities.Where(entity => entity.Count > 0 || !entity.IsSuccessful).Select(McpMapping.ToInfo).ToArray());
        });

    /// <summary>Records a complete scan only (a partial one would draw a false dip); history is best effort.</summary>
    private async Task RecordHistoryAsync(DeadLetterSnapshot snapshot, string environmentName, CancellationToken cancellationToken)
    {
        if (History is null || snapshot.HasFailures)
        {
            return;
        }
        try
        {
            await History.AppendAsync(DeadLetterHistorySample.FromSnapshot(snapshot, environmentName), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            // The scan itself succeeded; a history file that cannot be written does not change its answer.
        }
    }

    [McpServerTool(Name = "get_dead_letter_history", Title = "Dead-letter history", ReadOnly = true, Idempotent = true, OpenWorld = false)]
    [Description("How the number of dead-lettered messages in an environment changed over time, from QueueLoom's own records " +
                 "(every monitor check and scan, kept 30 days). Returns the count now, at the start of the period and at its peak, " +
                 "up to 48 points over time and the queues that changed most. Use it to answer 'when did this start' or 'is it getting worse'.")]
    public Task<DeadLetterHistoryInfo> GetDeadLetterHistoryAsync(
        [Description(EnvironmentDescription)] string? environment = null,
        [Description("How many hours back to look, 1-720 (30 days). Default 24.")] int hours = 24,
        CancellationToken cancellationToken = default) =>
        McpGuard.RunAsync(async () =>
        {
            var history = History ?? throw new InvalidOperationException("Dead-letter history is not available in this QueueLoom.");
            if (hours is < 1 or > 720)
            {
                throw new InvalidOperationException("Use 1-720 hours.");
            }
            var profile = await session.ResolveProfileAsync(environment, cancellationToken).ConfigureAwait(false);
            var to = DateTimeOffset.UtcNow;
            var from = to.AddHours(-hours);
            var summary = DeadLetterHistory.Summarize(await history.ReadAsync(profile.Id, from, cancellationToken).ConfigureAwait(false), from, to, maximumPoints: 48, maximumSources: 10);
            return summary is null
                ? new DeadLetterHistoryInfo(profile.Name, from, to, 0, null, null, null, null, [], [],
                    "Nothing was recorded in this period. History grows while a QueueLoom monitor runs or when dead letters are scanned.")
                : new DeadLetterHistoryInfo(profile.Name, from, to, summary.SampleCount, summary.Now, summary.Start, summary.Change,
                    new DeadLetterHistoryPointInfo(summary.Peak.At, summary.Peak.Count),
                    summary.Points.Select(point => new DeadLetterHistoryPointInfo(point.At, point.Count)).ToArray(),
                    summary.Sources.Select(source => new DeadLetterTrendInfo(source.Name, source.Start, source.Now, source.Change)).ToArray(),
                    null);
        });

    [McpServerTool(Name = "check_topic_routing", Title = "Subscription rules and routing", ReadOnly = true, Idempotent = true)]
    [Description("Lists every subscription of a topic with the rules that decide what it receives: Azure Service Bus SQL and " +
                 "correlation rules, Amazon SNS filter policies, Google Pub/Sub filters, or the bindings of a RabbitMQ exchange " +
                 "(pass the exchange as topic; the routing key is subject). When message fields are given, also says which " +
                 "subscriptions would receive such a message and, for the others, which comparison failed. Use it when a message " +
                 "'disappeared': a message no subscription matches is dropped silently. Service Bus compares property names " +
                 "case-insensitively; SNS, Pub/Sub and RabbitMQ are case-sensitive.")]
    public Task<TopicRoutingInfo> CheckTopicRoutingAsync(
        [Description("Topic name.")] string topic,
        [Description(EnvironmentDescription)] string? environment = null,
        [Description("Subject (sys.Label) of the message to check; the routing key in RabbitMQ.")] string? subject = null,
        [Description("Correlation ID of the message to check.")] string? correlationId = null,
        [Description("Message ID of the message to check.")] string? messageId = null,
        [Description("Content type of the message to check.")] string? contentType = null,
        [Description("'To' of the message to check.")] string? to = null,
        [Description("Session ID of the message to check.")] string? sessionId = null,
        [Description("Application properties of the message to check, as a JSON object; strings, numbers and booleans keep their type, " +
                     "for example {\"region\": \"EU\", \"amount\": 250}.")] Dictionary<string, System.Text.Json.JsonElement>? properties = null,
        [Description("Body of the message to check, for SNS filter policies on the message body.")] string? body = null,
        CancellationToken cancellationToken = default) =>
        McpGuard.RunAsync(async () =>
        {
            var profile = await session.ResolveProfileAsync(environment, cancellationToken).ConfigureAwait(false);
            var (rules, service) = await session.ReadAsync(profile, async (workspace, token) => workspace.SupportsSubscriptionRules
                    ? (await workspace.GetTopicRulesAsync(topic, token).ConfigureAwait(false), workspace.RoutingService)
                    : throw new InvalidOperationException($"{profile.Provider.DisplayName()} has no subscription rules or bindings."),
                cancellationToken).ConfigureAwait(false);
            var applicationProperties = (properties ?? []).Select(pair => pair.Value.ValueKind switch
            {
                System.Text.Json.JsonValueKind.True or System.Text.Json.JsonValueKind.False =>
                    new MessageApplicationProperty(pair.Key, ApplicationPropertyType.Boolean, pair.Value.GetBoolean() ? "true" : "false"),
                System.Text.Json.JsonValueKind.Number when pair.Value.TryGetInt64(out var integer) =>
                    new MessageApplicationProperty(pair.Key, ApplicationPropertyType.Int64, integer.ToString(CultureInfo.InvariantCulture)),
                System.Text.Json.JsonValueKind.Number =>
                    new MessageApplicationProperty(pair.Key, ApplicationPropertyType.Double, pair.Value.GetDouble().ToString(CultureInfo.InvariantCulture)),
                _ => new MessageApplicationProperty(pair.Key, ApplicationPropertyType.String, pair.Value.ToString())
            }).ToArray();
            var hasMessage = subject is not null || correlationId is not null || messageId is not null || contentType is not null ||
                             to is not null || sessionId is not null || applicationProperties.Length > 0 || body is not null;
            var routing = hasMessage
                ? QueueLoom.Core.Routing.TopicRouting.Route(topic, rules, new QueueLoom.Core.Routing.RoutingMessage(
                    new EditableMessageProperties(messageId, correlationId, contentType, subject, to, SessionId: sessionId), applicationProperties)
                {
                    Body = body
                }, service)
                : null;
            return new TopicRoutingInfo(profile.Name, topic, routing?.Headline,
                // Results come back in the order of the rules; a RabbitMQ queue and exchange may share a name.
                rules.Select((subscription, index) =>
                {
                    var result = routing?.Subscriptions[index];
                    return new SubscriptionRoutingInfo(subscription.Subscription,
                        subscription.Rules.Select(rule => new RuleInfo(rule.DisplayName, rule.KindLabel, rule.FilterText, rule.Action)).ToArray(),
                        subscription.Warning,
                        result?.Outcome.ToString(),
                        result?.Summary)
                    {
                        // A 1=1 rule that makes the others moot, or a filter QueueLoom cannot read, is said along with the note.
                        Note = string.Join(" · ", new[] { subscription.Note, subscription.Unreadable, subscription.CatchAllNotice }.OfType<string>()) is { Length: > 0 } note
                            ? note : null,
                        IsExchange = service == QueueLoom.Core.Routing.RoutingService.RabbitMq ? subscription.IsExchange : null
                    };
                }).ToArray());
        });

    [McpServerTool(Name = "peek_messages", Title = "Peek messages", ReadOnly = true, Idempotent = true)]
    [Description("Returns messages from a queue or subscription without removing them. Azure Service Bus peeks; " +
                 "SQS, Pub/Sub and RabbitMQ receive the messages and release them at once (paging is not available there). " +
                 "That read counts as a delivery: it raises the SQS receive count, the Pub/Sub delivery attempts (with a dead-letter policy) and, up to RabbitMQ 4.2, " +
                 "a quorum queue's delivery count, so a redrive or dead-letter policy can move a message that is read often " +
                 "(or, with no dead-letter target, such as a RabbitMQ quorum queue without a dead-letter exchange, drop it). " +
                 "Bodies longer than 4,000 characters are truncated, as are property values over 1,000 characters (at most 50 properties) " +
                 "and dead-letter reasons or descriptions over 4,000; the *Truncated fields say when. Packed bodies (gzip, base64, Avro, Protobuf) are also returned " +
                 "unpacked in decodedBody. Use fromSequenceNumber to page on Azure. Reading 'active' on SQS, Pub/Sub or RabbitMQ " +
                 "asks the user first, because those reads count as deliveries of live messages.")]
    public Task<MessageListInfo> PeekMessagesAsync(
        McpServer server,
        [Description("Queue name, or 'topic/subscription'.")] string entity,
        [Description(EnvironmentDescription)] string? environment = null,
        [Description("'dlq' (default), 'transfer-dlq' or 'active'.")] string subQueue = "dlq",
        [Description("How many messages to return, 1-100.")] int maxMessages = 20,
        [Description("Start at this sequence number (for paging).")] long? fromSequenceNumber = null,
        CancellationToken cancellationToken = default) =>
        McpGuard.RunAsync(async () =>
        {
            var profile = await session.ResolveProfileAsync(environment, cancellationToken).ConfigureAwait(false);
            var queue = McpMapping.ParseSubQueue(subQueue);
            var count = Math.Clamp(maxMessages, 1, 100);
            await ConfirmLiveQueueReadAsync(server, profile, entity, queue, count, cancellationToken).ConfigureAwait(false);
            var cleanup = new List<string>();
            var messages = await session.ReadAsync(profile, async (workspace, token) =>
            {
                var topology = await workspace.GetTopologyAsync(forceRefresh: false, token).ConfigureAwait(false);
                var source = EntityResolver.Resolve(topology, entity, requireMessageSource: true);
                return await workspace.BrowseMessagesAsync(
                        new BrowseMessagesRequest(source, queue, count, fromSequenceNumber), token)
                    .ConfigureAwait(false);
            }, cancellationToken, cleanup).ConfigureAwait(false);
            return new MessageListInfo(
                profile.Name,
                $"{messages.Count} message(s) from {entity} ({McpMapping.SubQueueName(queue)}); nothing was removed" +
                (profile.Provider is MessagingProvider.AzureServiceBus or MessagingProvider.Kafka
                    ? "."
                    // These services have no peek: messages are received, held briefly and released, which counts as a
                    // receive (SQS), a delivery attempt (Pub/Sub with a dead-letter policy) or a requeue (RabbitMQ).
                    : $"; {profile.Provider.DisplayName()} counts each read as a delivery, so it can move messages to a dead-letter queue (or drop them where none is set).") +
                McpMapping.CleanupNote(cleanup),
                messages.Select(McpMapping.ToInfo).ToArray());
        });

    [McpServerTool(Name = "search_dead_letters", Title = "Search dead letters", ReadOnly = true, Idempotent = true)]
    [Description("Searches every dead-letter queue of the environment: text in the Message ID, Correlation ID, subject, " +
                 "application properties or body (first 1 MiB), a /regular expression/, or a condition on a field of the JSON body. " +
                 "Results are capped like peek_messages (the *Truncated fields say when) and can be passed to delete_dead_letter_messages. " +
                 "Like peek_messages, it reads every scanned message, which counts as a delivery on SQS, on Pub/Sub with a dead-letter policy and on RabbitMQ quorum queues up to 4.2.")]
    public Task<MessageListInfo> SearchDeadLettersAsync(
        [Description(QueryDescription)] string query,
        [Description(EnvironmentDescription)] string? environment = null,
        [Description("Maximum number of matches, 1-500.")] int maxResults = 50,
        CancellationToken cancellationToken = default) =>
        McpGuard.RunAsync(async () =>
        {
            if (string.IsNullOrWhiteSpace(query))
            {
                throw new McpException("A search query is required.");
            }

            var profile = await session.ResolveProfileAsync(environment, cancellationToken).ConfigureAwait(false);
            var cleanup = new List<string>();
            var result = await session.ReadAsync(profile, async (workspace, token) =>
            {
                var topology = await workspace.GetTopologyAsync(forceRefresh: true, token).ConfigureAwait(false);
                var targets = DeadLetterSearchTargets.ForTopology(topology);
                return targets.Length == 0
                    ? null
                    : await workspace.SearchDeadLettersAsync(
                            new DeadLetterSearchRequest(query.Trim(), targets, maximumResults: Math.Clamp(maxResults, 1, 500)), token)
                        .ConfigureAwait(false);
            }, cancellationToken, cleanup).ConfigureAwait(false);
            if (result is null)
            {
                return new MessageListInfo(profile.Name, "The environment has no queues or subscriptions.", []);
            }

            return new MessageListInfo(
                profile.Name,
                $"{result.MatchCount} match(es) after inspecting {result.ScannedMessageCount} message(s)" +
                (result.IsComplete ? "." : "; the search stopped at a limit or hit errors, so results may be incomplete.") +
                McpMapping.CleanupNote(cleanup),
                result.Matches.Select(McpMapping.ToInfo).ToArray());
        });

    [McpServerTool(Name = "export_messages", Title = "Export messages to a file", ReadOnly = true, Idempotent = false, OpenWorld = false)]
    [Description("Saves messages with their full bodies and properties to a JSON or CSV file on this computer and returns its path. " +
                 "Pass 'entity' to export messages from one queue or subscription (like peek_messages), or 'query' to export " +
                 "dead letters matching a search across the environment (like search_dead_letters). Nothing is removed. " +
                 "Exporting 'active' on SQS, Pub/Sub or RabbitMQ asks the user first, as peek_messages does.")]
    public Task<ExportInfo> ExportMessagesAsync(
        McpServer server,
        [Description(EnvironmentDescription)] string? environment = null,
        [Description("Queue name, or 'topic/subscription'.")] string? entity = null,
        [Description("Search the dead-letter queues instead of reading 'entity'. " + QueryDescription)] string? query = null,
        [Description("With 'entity': 'dlq' (default), 'transfer-dlq' or 'active'.")] string subQueue = "dlq",
        [Description("How many messages at most, 1-1,000.")] int maxMessages = 100,
        [Description("'json' (default; complete) or 'csv' (one row per message, for spreadsheets).")] string format = "json",
        [Description("Optional file name without folders; a name with the environment and time is chosen when omitted.")] string? fileName = null,
        CancellationToken cancellationToken = default) =>
        McpGuard.RunAsync(async () =>
        {
            var hasEntity = !string.IsNullOrWhiteSpace(entity);
            var hasQuery = !string.IsNullOrWhiteSpace(query);
            if (hasEntity == hasQuery)
            {
                throw new McpException("Pass either 'entity' or 'query'.");
            }
            var extension = format?.Trim().ToLowerInvariant() switch
            {
                "json" or "" or null => ".json",
                "csv" => ".csv",
                _ => throw new McpException("format must be 'json' or 'csv'.")
            };
            var count = Math.Clamp(maxMessages, 1, BrowseMessagesRequest.MaximumMaxMessages);

            var profile = await session.ResolveProfileAsync(environment, cancellationToken).ConfigureAwait(false);
            if (hasEntity)
            {
                await ConfirmLiveQueueReadAsync(server, profile, entity!, McpMapping.ParseSubQueue(subQueue), count, cancellationToken)
                    .ConfigureAwait(false);
            }
            var (label, messages, complete) = await session.ReadAsync(profile, async (workspace, token) =>
            {
                var topology = await workspace.GetTopologyAsync(forceRefresh: hasQuery, token).ConfigureAwait(false);
                if (hasEntity)
                {
                    var source = EntityResolver.Resolve(topology, entity!, requireMessageSource: true);
                    var queue = McpMapping.ParseSubQueue(subQueue);
                    var browsed = await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(source, queue, count), token)
                        .ConfigureAwait(false);
                    return ($"{McpMapping.EntityName(source)}-{McpMapping.SubQueueName(queue)}", browsed, true);
                }

                var targets = DeadLetterSearchTargets.ForTopology(topology);
                if (targets.Length == 0)
                {
                    return ("search", (IReadOnlyList<BrowsedMessage>)[], true);
                }
                var result = await workspace.SearchDeadLettersAsync(
                        new DeadLetterSearchRequest(query!.Trim(), targets, maximumResults: count), token)
                    .ConfigureAwait(false);
                return ("search", result.Matches, result.IsComplete);
            }, cancellationToken).ConfigureAwait(false);

            // The name is reserved (an empty file created exclusively) before writing, so two exports choosing the same
            // name at the same moment get two files; the export then replaces only its own reservation.
            var path = ExportPath(profile.Name, label, fileName, extension);
            try
            {
                await MessageExport.WriteAsync(path, messages.Select(message => new ExportedMessage(profile.Name, message)).ToArray(),
                    cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                ReleaseReservation(path);
                throw;
            }
            session.Record("Info", "Exported messages", $"{messages.Count} message(s) to {path}", profile);
            return new ExportInfo(
                profile.Name,
                path,
                messages.Count,
                extension.TrimStart('.'),
                $"{messages.Count} message(s) saved to {path}" +
                (messages.Any(message => message.IsBodyTruncated) ? "; some bodies were too large and are truncated" : string.Empty) +
                (complete ? "." : "; the search stopped at a limit or hit errors, so the file may be incomplete."));
        });

    private string ExportPath(string environment, string label, string? fileName, string extension)
    {
        var directory = settings.ResolvedExportDirectory;
        Directory.CreateDirectory(directory);
        var name = string.IsNullOrWhiteSpace(fileName)
            ? $"{environment}-{label}-{DateTimeOffset.Now.ToString("yyyyMMdd-HHmmss", CultureInfo.InvariantCulture)}"
            : Path.GetFileNameWithoutExtension(Path.GetFileName(fileName.Trim()));
        var safe = new string(name.Select(character =>
            Path.GetInvalidFileNameChars().Contains(character) || character is '/' or '\\' or ':' ? '-' : character).ToArray()).Trim(' ', '.');
        if (safe.Length == 0)
        {
            safe = "messages";
        }

        // Checking File.Exists and writing later let two exports in the same second pick the same name and the later
        // move replace the earlier file. CreateNew claims the name atomically: whoever creates it owns it.
        for (var copy = 1; ; copy++)
        {
            var path = Path.Combine(directory, copy == 1 ? safe + extension : $"{safe} ({copy}){extension}");
            try
            {
                // Claimed private (owner-only), so the export written over it stays private as well.
                QueueLoom.Core.IO.SafeFileWriter.CreateEmptyPrivate(path);
                return path;
            }
            catch (IOException) when (File.Exists(path) || Directory.Exists(path))
            {
                // Taken by an earlier export or another one running now: try the next number.
            }
        }
    }

    /// <summary>Removes a reserved name the export never filled (still empty), so a failed export leaves no file.</summary>
    private static void ReleaseReservation(string path)
    {
        try
        {
            if (new FileInfo(path) is { Exists: true, Length: 0 })
            {
                File.Delete(path);
            }
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            // The export's own failure is what the caller needs to see.
        }
    }

    [McpServerTool(Name = "explain_dead_letters", Title = "Why dead letters pile up", ReadOnly = true, Idempotent = true)]
    [Description("Reads the dead-letter queues of the environment (or of one queue or subscription) and groups the messages by cause: " +
                 "the dead-letter reason plus the shape of the error description, with IDs and numbers left out (for example " +
                 "'Order {n} was not found'). For each cause: how many, where, since when, an example, sample message IDs and what " +
                 "the reason usually means. Start here when asked why messages are dead-lettered. Nothing is locked or removed.")]
    public Task<DeadLetterExplanationInfo> ExplainDeadLettersAsync(
        [Description(EnvironmentDescription)] string? environment = null,
        [Description("Queue name or 'topic/subscription'; every dead-letter queue when omitted.")] string? entity = null,
        [Description("How many dead letters to read per queue, 1-1,000.")] int maxMessagesPerQueue = 200,
        [Description("How many causes to return, most frequent first, 1-50.")] int maxCauses = 15,
        CancellationToken cancellationToken = default) =>
        McpGuard.RunAsync(async () =>
        {
            var perQueue = Math.Clamp(maxMessagesPerQueue, 1, BrowseMessagesRequest.MaximumMaxMessages);
            var profile = await session.ResolveProfileAsync(environment, cancellationToken).ConfigureAwait(false);
            var (sources, messages) = await session.ReadAsync(profile, async (workspace, token) =>
            {
                var topology = await workspace.GetTopologyAsync(forceRefresh: true, token).ConfigureAwait(false);
                var queues = topology.Queues.Where(queue => queue.HasDeadLetterQueue)
                    .Select(queue => (queue.Reference, Count: queue.Runtime.MessageCounts.DeadLetter, queue.Note))
                    .Concat(topology.Topics.SelectMany(topic => topic.Subscriptions).Where(subscription => subscription.HasDeadLetterQueue)
                        .Select(subscription => (subscription.Reference, Count: subscription.Runtime.MessageCounts.DeadLetter, subscription.Note)))
                    .ToArray();
                if (!string.IsNullOrWhiteSpace(entity))
                {
                    var wanted = EntityResolver.Resolve(topology, entity, requireMessageSource: true);
                    queues = queues.Where(queue => queue.Reference == wanted).ToArray();
                }
                else if (topology.HasMessageCounts)
                {
                    queues = queues.Where(queue => queue.Count > 0).ToArray();
                }

                var summaries = new List<DeadLetterSourceSummaryInfo>();
                var read = new List<BrowsedMessage>();
                foreach (var (reference, count, note) in queues)
                {
                    try
                    {
                        var browsed = await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(reference, ServiceBusSubQueue.DeadLetter, perQueue), token)
                            .ConfigureAwait(false);
                        read.AddRange(browsed);
                        summaries.Add(new DeadLetterSourceSummaryInfo(McpMapping.EntityName(reference), count, browsed.Count, note, null));
                    }
                    catch (Exception exception) when (exception is InvalidOperationException or NotSupportedException or TimeoutException)
                    {
                        summaries.Add(new DeadLetterSourceSummaryInfo(McpMapping.EntityName(reference), count, 0, note,
                            QueueLoom.Core.Diagnostics.SensitiveDataRedactor.SummarizeException(exception)));
                    }
                }
                return (summaries, read);
            }, cancellationToken).ConfigureAwait(false);

            var total = messages.Count;
            var causes = messages
                .GroupBy(DeadLetterCauses.KeyOf)
                .Select(group => new DeadLetterCauseInfo(
                    group.Key.Reason,
                    group.Key.Pattern,
                    group.Select(message => message.DeadLetterErrorDescription).FirstOrDefault(text => !string.IsNullOrWhiteSpace(text))?.Trim().Split('\n')[0],
                    group.Count(),
                    Math.Round(100.0 * group.Count() / Math.Max(total, 1), 1),
                    group.GroupBy(message => McpMapping.EntityName(message.Source))
                        .OrderByDescending(source => source.Count())
                        .ToDictionary(source => source.Key, source => source.Count()),
                    group.Min(message => message.EnqueuedAt),
                    group.Max(message => message.EnqueuedAt),
                    group.Max(message => message.DeliveryCount),
                    group.Select(message => message.Properties.MessageId).OfType<string>().Take(3).ToArray(),
                    DeadLetterHints.For(group.Key.Reason)))
                .OrderByDescending(cause => cause.Count)
                .Take(Math.Clamp(maxCauses, 1, 50))
                .ToArray();
            var summary = total == 0
                ? sources.Count == 0 ? "No dead-letter queue holds messages." : "The dead-letter queues that were read are empty."
                : $"{total:N0} dead letter(s) read from {sources.Count(source => source.Read > 0):N0} queue(s) fall into " +
                  $"{messages.GroupBy(DeadLetterCauses.KeyOf).Count():N0} cause(s); the largest is {causes[0].Reason}" +
                  (causes[0].Pattern is null ? string.Empty : $": {causes[0].Pattern}") + $" ({causes[0].Share}%)." +
                  (sources.Any(source => source.DeadLetterCount > source.Read) ? " Some queues hold more than was read; raise maxMessagesPerQueue to read more." : string.Empty);
            return new DeadLetterExplanationInfo(profile.Name, summary, total, sources, causes);
        });

    [McpServerTool(Name = "trace_forwarding", Title = "Where forwarded messages go", ReadOnly = true, Idempotent = true)]
    [Description("Azure Service Bus auto-forwarding: follows a queue, topic or subscription through every forward (topics copy to their " +
                 "subscriptions, which may forward again) and says where messages end up, whether a chain loops, points at an entity " +
                 "that does not exist, or is longer than the 4 forwards Service Bus allows (messages are dead-lettered then).")]
    public Task<ForwardingInfo> TraceForwardingAsync(
        [Description("Queue, topic, or 'topic/subscription' to start from.")] string entity,
        [Description(EnvironmentDescription)] string? environment = null,
        CancellationToken cancellationToken = default) =>
        McpGuard.RunAsync(async () =>
        {
            var profile = await session.ResolveProfileAsync(environment, cancellationToken).ConfigureAwait(false);
            var (start, report) = await session.ReadAsync(profile, async (workspace, token) =>
            {
                var topology = await workspace.GetTopologyAsync(forceRefresh: false, token).ConfigureAwait(false);
                var reference = EntityResolver.Resolve(topology, entity, requireMessageSource: false);
                var name = McpMapping.EntityName(reference);
                return (name, QueueLoom.Core.Routing.Forwarding.Follow(topology, name));
            }, cancellationToken).ConfigureAwait(false);

            static string Line(IReadOnlyList<string> path) => string.Join(" → ", path);
            var summary = report.Loops.Count > 0 ? $"Loop: {Line(report.Loops[0])}. Service Bus dead-letters these messages after 4 forwards."
                : report.Missing.Count > 0 ? $"{Line(report.Missing[0])} points at an entity that does not exist."
                : report.LongestChain == 0 ? $"{start} does not forward: messages stay there."
                : report.IsTooLong ? $"{report.LongestChain} forwards is more than Service Bus allows (4): messages are dead-lettered on the way."
                : $"Messages end up in {string.Join(", ", report.Destinations)} after at most {report.LongestChain} forward(s).";
            return new ForwardingInfo(profile.Name, start, summary, report.Destinations, report.Paths.Select(Line).ToArray(),
                report.Loops.Select(Line).ToArray(), report.Missing.Select(Line).ToArray(), report.LongestChain);
        });
}
