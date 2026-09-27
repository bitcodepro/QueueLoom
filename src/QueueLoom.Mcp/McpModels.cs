using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Mcp;

// Shapes returned to MCP clients. Names are stable: models and prompts depend on them.

public sealed record EnvironmentInfo(string Name, string Id, string Kind, string? Namespace, string Authentication, string AccessMode);

public sealed record EntityInfo(
    string Kind,
    string Entity,
    string Status,
    long Active,
    long DeadLetter,
    long TransferDeadLetter,
    long Scheduled);

public sealed record TopologyInfo(string Environment, DateTimeOffset FetchedAt, bool CountsAreSampled, IReadOnlyList<EntityInfo> Entities);

public sealed record DeadLetterSourceInfo(string Entity, string SubQueue, long? Count, string? Error);

public sealed record DeadLetterScanInfo(string Environment, DateTimeOffset CapturedAt, long TotalCount, IReadOnlyList<DeadLetterSourceInfo> Sources);

public sealed record MessageInfo(
    string Entity,
    string SubQueue,
    long SequenceNumber,
    string? MessageId,
    string? CorrelationId,
    string? Subject,
    string? ContentType,
    DateTimeOffset? EnqueuedAt,
    int DeliveryCount,
    string? DeadLetterReason,
    string? DeadLetterDescription,
    IReadOnlyDictionary<string, string> ApplicationProperties,
    string BodyFormat,
    string Body,
    bool BodyTruncated);

public sealed record MessageListInfo(string Environment, string Summary, IReadOnlyList<MessageInfo> Messages);

public sealed record MessageSelection(string Entity, string SubQueue, long SequenceNumber, string? MessageId);

public sealed record ChangeResult(string Environment, bool Approved, string Summary, string? BackupDirectory, IReadOnlyList<string> Details);

internal static class McpMapping
{
    private const int MaximumBodyCharacters = 4_000;

    public static EnvironmentInfo ToInfo(ServiceBusProfile profile) => new(
        profile.Name,
        profile.Id.ToString("D"),
        profile.EnvironmentDisplayName,
        profile.FullyQualifiedNamespace,
        profile.Authentication.Kind == AuthenticationKind.ConnectionString ? "Connection string" : "Microsoft Entra ID",
        profile.AccessMode.ToString());

    public static string SubQueueName(ServiceBusSubQueue subQueue) => subQueue switch
    {
        ServiceBusSubQueue.DeadLetter => "dlq",
        ServiceBusSubQueue.TransferDeadLetter => "transfer-dlq",
        _ => "active"
    };

    public static ServiceBusSubQueue ParseSubQueue(string? value) => value?.Trim().ToLowerInvariant() switch
    {
        null or "" or "dlq" or "deadletter" or "dead-letter" => ServiceBusSubQueue.DeadLetter,
        "transfer-dlq" or "transferdeadletter" or "transfer-dead-letter" => ServiceBusSubQueue.TransferDeadLetter,
        "active" => ServiceBusSubQueue.Active,
        _ => throw new ModelContextProtocol.McpException($"Unknown sub-queue '{value}'. Use 'dlq', 'transfer-dlq' or 'active'.")
    };

    public static EntityInfo ToInfo(ServiceBusEntityReference reference, ServiceBusEntityRuntime runtime, ServiceBusEntityStatus status) => new(
        reference.Kind.ToString(),
        EntityName(reference),
        status.ToString(),
        runtime.MessageCounts.Active,
        runtime.MessageCounts.DeadLetter,
        runtime.MessageCounts.TransferDeadLetter,
        runtime.MessageCounts.Scheduled);

    /// <summary>"orders" for a queue or topic, "topic/subscription" for a subscription.</summary>
    public static string EntityName(ServiceBusEntityReference reference) =>
        reference.Kind == ServiceBusEntityKind.Subscription ? $"{reference.TopicName}/{reference.Name}" : reference.Name;

    public static DeadLetterSourceInfo ToInfo(DeadLetterEntitySnapshot snapshot) =>
        new(EntityName(snapshot.Entity), SubQueueName(snapshot.SubQueue), snapshot.Count, snapshot.Error);

    public static MessageInfo ToInfo(BrowsedMessage message)
    {
        var body = EditableMessageBody.FromBytes(message.Body.Span);
        var text = body.Content;
        var truncated = message.IsBodyTruncated || text.Length > MaximumBodyCharacters;
        if (text.Length > MaximumBodyCharacters)
        {
            text = text[..MaximumBodyCharacters];
        }

        return new MessageInfo(
            EntityName(message.Source),
            SubQueueName(message.SubQueue),
            message.SequenceNumber,
            message.Properties.MessageId,
            message.Properties.CorrelationId,
            message.Properties.Subject,
            message.Properties.ContentType,
            message.EnqueuedAt,
            message.DeliveryCount,
            message.DeadLetterReason,
            message.DeadLetterErrorDescription,
            message.ApplicationProperties.ToDictionary(property => property.Name, property => property.Value),
            body.Format.ToString(),
            text,
            truncated);
    }
}

/// <summary>Resolves entity names given by a model ("orders", "topic/subscription", or a full path) against the topology.</summary>
internal static class EntityResolver
{
    public static ServiceBusEntityReference Resolve(ServiceBusTopology topology, string entity, bool requireMessageSource)
    {
        if (string.IsNullOrWhiteSpace(entity))
        {
            throw new ModelContextProtocol.McpException("An entity name is required.");
        }

        var key = entity.Trim().Replace(" / ", "/", StringComparison.Ordinal);
        var candidates = topology.Queues.Select(queue => queue.Reference)
            .Concat(topology.Topics.Select(topic => topic.Reference))
            .Concat(topology.Topics.SelectMany(topic => topic.Subscriptions).Select(subscription => subscription.Reference));
        var match = candidates.FirstOrDefault(reference =>
            string.Equals(McpMapping.EntityName(reference), key, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(reference.Path, key, StringComparison.OrdinalIgnoreCase));
        if (match is null)
        {
            throw new ModelContextProtocol.McpException(
                $"Entity '{entity}' was not found. Use get_entities to list queues ('name') and subscriptions ('topic/subscription').");
        }
        if (requireMessageSource && !match.CanBrowse)
        {
            throw new ModelContextProtocol.McpException(
                $"'{entity}' is a topic; messages live in its subscriptions ('{entity}/<subscription>').");
        }
        return match;
    }
}
