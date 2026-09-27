namespace QueueLoom.Core.ServiceBus;

public sealed record ServiceBusSubscription(
    string TopicName,
    string Name,
    ServiceBusEntityRuntime Runtime,
    ServiceBusEntityStatus Status = ServiceBusEntityStatus.Unknown,
    bool RequiresSession = false)
{
    /// <summary>False when the subscription has no dead-letter destination that QueueLoom can read.</summary>
    public bool HasDeadLetterQueue { get; init; } = true;

    /// <summary>A short provider-specific note, for example the SNS endpoint or the Pub/Sub dead-letter topic.</summary>
    public string? Note { get; init; }

    public ServiceBusEntityReference Reference =>
        ServiceBusEntityReference.Subscription(TopicName, Name);
}
