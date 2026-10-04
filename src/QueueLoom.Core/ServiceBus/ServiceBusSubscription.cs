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

    /// <summary>
    /// Amazon SQS FIFO: while a read holds messages of a message group, SQS returns no further messages of that group,
    /// so a read sees at most one receive batch (10 messages) per group and cannot tell whether more remain.
    /// </summary>
    public bool ReadsOneBatchPerMessageGroup { get; init; }

    /// <summary>Azure Service Bus auto-forwarding: the queue or topic every message is moved to at once, or null.</summary>
    public string? ForwardTo { get; init; }

    /// <summary>Azure Service Bus: where dead letters are forwarded instead of staying in the dead-letter queue, or null.</summary>
    public string? ForwardDeadLettersTo { get; init; }

    public ServiceBusEntityReference Reference =>
        ServiceBusEntityReference.Subscription(TopicName, Name);
}
