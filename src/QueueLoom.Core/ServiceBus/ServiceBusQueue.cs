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

    /// <summary>
    /// Amazon SQS FIFO: while a read holds messages of a message group, SQS returns no further messages of that group,
    /// so a read sees at most one receive batch (10 messages) per group and cannot tell whether more remain.
    /// </summary>
    public bool ReadsOneBatchPerMessageGroup { get; init; }

    /// <summary>Connected consumers (RabbitMQ) or consumer group lag (Kafka); null where the service does not say.</summary>
    public ConsumerActivity? Consumers { get; init; }

    /// <summary>Azure Service Bus auto-forwarding: the queue or topic every message is moved to at once, or null.</summary>
    public string? ForwardTo { get; init; }

    /// <summary>Azure Service Bus: where dead letters are forwarded instead of staying in the dead-letter queue, or null.</summary>
    public string? ForwardDeadLettersTo { get; init; }

    /// <summary>
    /// How many deliveries the service allows a message of this queue before it dead-letters it, where reading counts as
    /// a delivery: the SQS redrive policy's maxReceiveCount or a RabbitMQ quorum queue's own delivery limit. Null when
    /// the queue has none or the service does not say.
    /// </summary>
    public int? MaxDeliveryCount { get; init; }

    /// <summary>
    /// RabbitMQ: a quorum queue with a delivery limit (its own, or the default of 20 since RabbitMQ 4.0). Up to RabbitMQ
    /// 4.2 every requeue counts toward that limit, including the basic.nack with requeue that ends QueueLoom's read.
    /// </summary>
    public bool CountsRequeues { get; init; }

    /// <summary>The queue of this topology its dead letters are read from (SQS, RabbitMQ), or null.</summary>
    public string? DeadLetterQueueName { get; init; }

    public ServiceBusEntityReference Reference => ServiceBusEntityReference.Queue(Name);
}
