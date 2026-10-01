namespace QueueLoom.Core.ServiceBus;

public sealed record ServiceBusQueue(
    string Name,
    ServiceBusEntityRuntime Runtime,
    ServiceBusEntityStatus Status = ServiceBusEntityStatus.Unknown,
    bool RequiresSession = false)
{
    /// <summary>False when the queue has no dead-letter queue configured (for example an SQS queue without a redrive policy).</summary>
    public bool HasDeadLetterQueue { get; init; } = true;

    /// <summary>A short provider-specific note shown next to the name, for example which queue this one is the DLQ of.</summary>
    public string? Note { get; init; }

    /// <summary>Connected consumers (RabbitMQ) or consumer group lag (Kafka); null where the service does not say.</summary>
    public ConsumerActivity? Consumers { get; init; }

    /// <summary>Azure Service Bus auto-forwarding: the queue or topic every message is moved to at once, or null.</summary>
    public string? ForwardTo { get; init; }

    /// <summary>Azure Service Bus: where dead letters are forwarded instead of staying in the dead-letter queue, or null.</summary>
    public string? ForwardDeadLettersTo { get; init; }

    public ServiceBusEntityReference Reference => ServiceBusEntityReference.Queue(Name);
}
