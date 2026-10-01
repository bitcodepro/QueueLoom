using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Mcp;

// Shapes returned to MCP clients. Names are stable: models and prompts depend on them.

/// <param name="Namespace">Where the environment lives: Service Bus namespace, AWS region or Google Cloud project.</param>
/// <param name="Service">Azure Service Bus, Amazon SQS / SNS or Google Cloud Pub/Sub.</param>
public sealed record EnvironmentInfo(
    string Name,
    string Id,
    string Kind,
    string? Namespace,
    string Authentication,
    string AccessMode,
    string Service);

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

public sealed record RuleInfo(string Name, string Kind, string Filter, string? Action);

/// <param name="Outcome">Receives, Skips or Unknown (only the service can tell), when a message was given.</param>
public sealed record SubscriptionRoutingInfo(string Subscription, IReadOnlyList<RuleInfo> Rules, string? Warning, string? Outcome, string? Explanation)
{
    /// <summary>For example "Alternate exchange: gets what no binding takes", or an SNS subscription's endpoint.</summary>
    public string? Note { get; init; }

    /// <summary>RabbitMQ: the destination is an exchange, not a queue (the two may share a name).</summary>
    public bool? IsExchange { get; init; }
}

/// <param name="Headline">For a given message: how many subscriptions receive it, or that it is dropped.</param>
public sealed record TopicRoutingInfo(string Environment, string Topic, string? Headline, IReadOnlyList<SubscriptionRoutingInfo> Subscriptions);

/// <param name="Hint">What this reason usually means and where to look, when QueueLoom knows the reason.</param>
public sealed record DeadLetterCauseInfo(
    string Reason,
    string? Pattern,
    string? Example,
    int Count,
    double Share,
    IReadOnlyDictionary<string, int> Sources,
    DateTimeOffset? FirstEnqueued,
    DateTimeOffset? LastEnqueued,
    int MaxDeliveryCount,
    IReadOnlyList<string> SampleMessageIds,
    string? Hint);

/// <param name="Note">What QueueLoom knows about the queue, such as where it forwards its dead letters.</param>
public sealed record DeadLetterSourceSummaryInfo(string Entity, long DeadLetterCount, int Read, string? Note, string? Error);

public sealed record DeadLetterExplanationInfo(
    string Environment,
    string Summary,
    int ReadMessages,
    IReadOnlyList<DeadLetterSourceSummaryInfo> Sources,
    IReadOnlyList<DeadLetterCauseInfo> Causes);

/// <param name="Paths">Every way a message travels, as "inbox → orders → orders/eu → eu-orders".</param>
public sealed record ForwardingInfo(
    string Environment,
    string Entity,
    string Summary,
    IReadOnlyList<string> Destinations,
    IReadOnlyList<string> Paths,
    IReadOnlyList<string> Loops,
    IReadOnlyList<string> Missing,
    int LongestChain);

public sealed record DeadLetterHistoryPointInfo(DateTimeOffset At, long Count);

public sealed record DeadLetterTrendInfo(string Source, long Start, long Now, long Change);

/// <param name="Note">Why there is nothing to show, when nothing was recorded.</param>
public sealed record DeadLetterHistoryInfo(
    string Environment,
    DateTimeOffset From,
    DateTimeOffset To,
    int SampleCount,
    long? Now,
    long? Start,
    long? Change,
    DeadLetterHistoryPointInfo? Peak,
    IReadOnlyList<DeadLetterHistoryPointInfo> Points,
    IReadOnlyList<DeadLetterTrendInfo> Sources,
    string? Note);

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
    bool BodyTruncated,
    string State,
    DateTimeOffset? ScheduledFor,
    string? DecodedAs = null,
    string? DecodedBody = null);

public sealed record MessageListInfo(string Environment, string Summary, IReadOnlyList<MessageInfo> Messages);

public sealed record MessageSelection(string Entity, string SubQueue, long SequenceNumber, string? MessageId);

/// <param name="Path">Full path of the file that was written.</param>
public sealed record ExportInfo(string Environment, string Path, int Count, string Format, string Summary);

public sealed record ChangeResult(string Environment, bool Approved, string Summary, string? BackupDirectory, IReadOnlyList<string> Details);

internal static class McpMapping
{
    private const int MaximumBodyCharacters = 4_000;

    public static EnvironmentInfo ToInfo(ServiceBusProfile profile) => new(
        profile.Name,
        profile.Id.ToString("D"),
        profile.EnvironmentDisplayName,
        profile.EndpointDisplay,
        profile.AuthenticationDisplayName,
        profile.AccessMode.ToString(),
        profile.Provider.DisplayName());

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
        var decoded = BodyDecoder.Decode(message.Body, message.Properties.ContentType, message.Schema,
            messageType: ProtoSchemaCatalog.HintFrom(message.Properties.ContentType, message.ApplicationProperties));
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
            truncated,
            message.State == ServiceBusMessageState.Unknown ? (message.IsDeadLetter ? "DeadLettered" : "Active") : message.State.ToString(),
            message.State == ServiceBusMessageState.Scheduled ? message.Properties.ScheduledEnqueueTime : null,
            decoded?.Summary,
            decoded is null ? null : decoded.Text.Length > MaximumBodyCharacters ? decoded.Text[..MaximumBodyCharacters] : decoded.Text);
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
