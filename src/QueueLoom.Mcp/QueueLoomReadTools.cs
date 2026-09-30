using System.ComponentModel;
using ModelContextProtocol;
using ModelContextProtocol.Server;
using System.Globalization;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Mcp;

/// <summary>Read-only tools. They never lock, settle or send messages, so they run without approval.</summary>
[McpServerToolType]
public sealed class QueueLoomReadTools(McpWorkspaceSession session, McpServerSettings settings, IServiceProvider services)
{
    private IDeadLetterHistoryStore? History => services.GetService(typeof(IDeadLetterHistoryStore)) as IDeadLetterHistoryStore;

    private const string EnvironmentDescription =
        "Saved environment name (see list_environments). Optional when only one environment is saved.";

    [McpServerTool(Name = "list_environments", Title = "List environments", ReadOnly = true, Idempotent = true, OpenWorld = false)]
    [Description("Lists the environments saved in QueueLoom (name, kind such as Production, service such as Amazon SQS / SNS, " +
        "namespace/region/project, authentication, access mode).")]
    public Task<IReadOnlyList<EnvironmentInfo>> ListEnvironmentsAsync(CancellationToken cancellationToken) =>
        McpGuard.RunAsync<IReadOnlyList<EnvironmentInfo>>(async () =>
            (await session.ListProfilesAsync(cancellationToken).ConfigureAwait(false)).Select(McpMapping.ToInfo).ToArray());

    [McpServerTool(Name = "get_entities", Title = "List queues, topics and subscriptions", ReadOnly = true, Idempotent = true)]
    [Description("Lists queues, topics and subscriptions with active, dead-letter, transfer dead-letter and scheduled message counts. " +
                 "Subscriptions are named 'topic/subscription'.")]
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
            History?.Append(DeadLetterHistorySample.FromSnapshot(snapshot, profile.Name));
            return new DeadLetterScanInfo(
                profile.Name,
                snapshot.CapturedAt,
                snapshot.TotalCount,
                snapshot.Entities.Where(entity => entity.Count > 0 || !entity.IsSuccessful).Select(McpMapping.ToInfo).ToArray());
        });

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
            var summary = DeadLetterHistory.Summarize(history.Read(profile.Id, from), from, to, maximumPoints: 48, maximumSources: 10);
            return summary is null
                ? new DeadLetterHistoryInfo(profile.Name, from, to, 0, null, null, null, null, [], [],
                    "Nothing was recorded in this period. History grows while a QueueLoom monitor runs or when dead letters are scanned.")
                : new DeadLetterHistoryInfo(profile.Name, from, to, summary.SampleCount, summary.Now, summary.Start, summary.Change,
                    new DeadLetterHistoryPointInfo(summary.Peak.At, summary.Peak.Count),
                    summary.Points.Select(point => new DeadLetterHistoryPointInfo(point.At, point.Count)).ToArray(),
                    summary.Sources.Select(source => new DeadLetterTrendInfo(source.Name, source.Start, source.Now, source.Change)).ToArray(),
                    null);
        });

    [McpServerTool(Name = "peek_messages", Title = "Peek messages", ReadOnly = true, Idempotent = true)]
    [Description("Returns messages from a queue or subscription without removing them. Azure Service Bus peeks; " +
                 "SQS and Pub/Sub receive the messages and release them at once (paging is not available there). " +
                 "Bodies longer than 4,000 characters are truncated. Packed bodies (gzip, base64, Avro, Protobuf) are also returned " +
                 "unpacked in decodedBody. Use fromSequenceNumber to page on Azure.")]
    public Task<MessageListInfo> PeekMessagesAsync(
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
            var messages = await session.ReadAsync(profile, async (workspace, token) =>
            {
                var topology = await workspace.GetTopologyAsync(forceRefresh: false, token).ConfigureAwait(false);
                var source = EntityResolver.Resolve(topology, entity, requireMessageSource: true);
                return await workspace.BrowseMessagesAsync(
                        new BrowseMessagesRequest(source, queue, count, fromSequenceNumber), token)
                    .ConfigureAwait(false);
            }, cancellationToken).ConfigureAwait(false);
            return new MessageListInfo(
                profile.Name,
                $"{messages.Count} message(s) from {entity} ({McpMapping.SubQueueName(queue)}); nothing was locked or removed.",
                messages.Select(McpMapping.ToInfo).ToArray());
        });

    [McpServerTool(Name = "search_dead_letters", Title = "Search dead letters", ReadOnly = true, Idempotent = true)]
    [Description("Searches every dead-letter queue of the environment for text in the Message ID, Correlation ID, subject, " +
                 "application properties or body (first 1 MiB). Results can be passed to delete_dead_letter_messages.")]
    public Task<MessageListInfo> SearchDeadLettersAsync(
        [Description("Text to look for.")] string query,
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
            var result = await session.ReadAsync(profile, async (workspace, token) =>
            {
                var topology = await workspace.GetTopologyAsync(forceRefresh: true, token).ConfigureAwait(false);
                var targets = DeadLetterSearchTargets.ForTopology(topology);
                return targets.Length == 0
                    ? null
                    : await workspace.SearchDeadLettersAsync(
                            new DeadLetterSearchRequest(query.Trim(), targets, maximumResults: Math.Clamp(maxResults, 1, 500)), token)
                        .ConfigureAwait(false);
            }, cancellationToken).ConfigureAwait(false);
            if (result is null)
            {
                return new MessageListInfo(profile.Name, "The environment has no queues or subscriptions.", []);
            }

            return new MessageListInfo(
                profile.Name,
                $"{result.MatchCount} match(es) after inspecting {result.ScannedMessageCount} message(s)" +
                (result.IsComplete ? "." : "; the search stopped at a limit or hit errors, so results may be incomplete."),
                result.Matches.Select(McpMapping.ToInfo).ToArray());
        });

    [McpServerTool(Name = "export_messages", Title = "Export messages to a file", ReadOnly = true, Idempotent = false, OpenWorld = false)]
    [Description("Saves messages with their full bodies and properties to a JSON or CSV file on this computer and returns its path. " +
                 "Pass 'entity' to export messages from one queue or subscription (like peek_messages), or 'query' to export " +
                 "dead letters matching a search across the environment (like search_dead_letters). Nothing is removed.")]
    public Task<ExportInfo> ExportMessagesAsync(
        [Description(EnvironmentDescription)] string? environment = null,
        [Description("Queue name, or 'topic/subscription'.")] string? entity = null,
        [Description("Text to search the dead-letter queues for, instead of 'entity'.")] string? query = null,
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

            var path = ExportPath(profile.Name, label, fileName, extension);
            await MessageExport.WriteAsync(path, messages.Select(message => new ExportedMessage(profile.Name, message)).ToArray(),
                cancellationToken).ConfigureAwait(false);
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

        var path = Path.Combine(directory, safe + extension);
        for (var copy = 2; File.Exists(path); copy++)
        {
            path = Path.Combine(directory, $"{safe} ({copy}){extension}");
        }
        return path;
    }
}
