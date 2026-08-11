namespace QueueLoom.Core.ServiceBus;

/// <summary>
/// Stable identity for a queue, topic, or subscription. It contains names only,
/// so it can safely be used as a navigation key or persisted UI selection.
/// </summary>
public sealed record ServiceBusEntityReference
{
    private const string SubscriptionsSegment = "Subscriptions";
    private const string DeadLetterSegment = "$DeadLetterQueue";
    private const string TransferDeadLetterSegment = "$Transfer/$DeadLetterQueue";

    private ServiceBusEntityReference(
        ServiceBusEntityKind kind,
        string name,
        string? topicName)
    {
        Kind = kind;
        Name = RequireName(name, nameof(name));
        TopicName = topicName is null ? null : RequireName(topicName, nameof(topicName));
    }

    public ServiceBusEntityKind Kind { get; }

    /// <summary>
    /// Queue/topic name, or subscription name for a subscription reference.
    /// </summary>
    public string Name { get; }

    public string? TopicName { get; }

    public string Path => Kind == ServiceBusEntityKind.Subscription
        ? $"{TopicName}/{SubscriptionsSegment}/{Name}"
        : Name;

    public string DisplayName => Kind == ServiceBusEntityKind.Subscription
        ? $"{TopicName} / {Name}"
        : Name;

    public bool CanBrowse => Kind is ServiceBusEntityKind.Queue or ServiceBusEntityKind.Subscription;

    public bool CanSend => Kind is ServiceBusEntityKind.Queue or ServiceBusEntityKind.Topic;

    public string? DeadLetterPath => CanBrowse ? $"{Path}/{DeadLetterSegment}" : null;

    public string? TransferDeadLetterPath => CanBrowse ? $"{Path}/{TransferDeadLetterSegment}" : null;

    public static ServiceBusEntityReference Queue(string name) =>
        new(ServiceBusEntityKind.Queue, name, null);

    public static ServiceBusEntityReference Topic(string name) =>
        new(ServiceBusEntityKind.Topic, name, null);

    public static ServiceBusEntityReference Subscription(string topicName, string subscriptionName) =>
        new(ServiceBusEntityKind.Subscription, subscriptionName, topicName);

    public override string ToString() => Path;

    private static string RequireName(string value, string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, parameterName);
        return value.Trim();
    }
}
