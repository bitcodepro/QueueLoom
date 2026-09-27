using System.ComponentModel;
using ModelContextProtocol;
using ModelContextProtocol.Server;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Mcp;

/// <summary>Read-only tools. They never lock, settle or send messages, so they run without approval.</summary>
[McpServerToolType]
public sealed class QueueLoomReadTools(McpWorkspaceSession session)
{
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
            var entities = topology.Queues.Select(queue => McpMapping.ToInfo(queue.Reference, queue.Runtime, queue.Status))
                .Concat(topology.Topics.SelectMany(topic =>
                    new[] { McpMapping.ToInfo(topic.Reference, topic.Runtime, topic.Status) }
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
            return new DeadLetterScanInfo(
                profile.Name,
                snapshot.CapturedAt,
                snapshot.TotalCount,
                snapshot.Entities.Where(entity => entity.Count > 0 || !entity.IsSuccessful).Select(McpMapping.ToInfo).ToArray());
        });

    [McpServerTool(Name = "peek_messages", Title = "Peek messages", ReadOnly = true, Idempotent = true)]
    [Description("Returns messages from a queue or subscription without removing them. Azure Service Bus peeks; " +
                 "SQS and Pub/Sub receive the messages and release them at once (paging is not available there). " +
                 "Bodies longer than 4,000 characters are truncated. Use fromSequenceNumber to page on Azure.")]
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
}
