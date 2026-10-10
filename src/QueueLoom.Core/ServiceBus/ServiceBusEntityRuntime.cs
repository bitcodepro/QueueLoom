namespace QueueLoom.Core.ServiceBus;

public sealed record ServiceBusEntityRuntime
{
    public ServiceBusEntityRuntime(
        ServiceBusMessageCounts messageCounts,
        long sizeInBytes = 0,
        DateTimeOffset? createdAt = null,
        DateTimeOffset? updatedAt = null,
        DateTimeOffset? accessedAt = null)
    {
        ArgumentNullException.ThrowIfNull(messageCounts);
        ArgumentOutOfRangeException.ThrowIfNegative(sizeInBytes);

        MessageCounts = messageCounts;
        SizeInBytes = sizeInBytes;
        CreatedAt = createdAt;
        UpdatedAt = updatedAt;
        AccessedAt = accessedAt;
    }

    public ServiceBusMessageCounts MessageCounts { get; }
    public bool IsEmulatorSample { get; init; }

    /// <summary>The service does not report counts for this entity, so every counter is unknown rather than zero.</summary>
    public bool CountsUnavailable { get; init; }

    /// <summary>
    /// The service reports these counts as approximate or delayed (SQS's ApproximateNumberOf…, Pub/Sub's Cloud
    /// Monitoring series): close, but not the exact number at this moment.
    /// </summary>
    public bool CountsAreEstimates { get; init; }

    /// <summary>When a reported dead-letter count was true, when it is older than the read (a Cloud Monitoring point).</summary>
    public DateTimeOffset? DeadLetterCountMeasuredAt { get; init; }

    /// <summary>What the dead-letter count was taken from, when not the entity itself (a Pub/Sub reader subscription).</summary>
    public string? DeadLetterCountSource { get; init; }

    /// <summary>A failed reported DLQ count; this must not become zero or trigger receive-based sampling.</summary>
    public string? DeadLetterCountError { get; init; }

    /// <summary>Only Azure Service Bus reports transfer dead-letter counts.</summary>
    public bool HasTransferDeadLetterCount { get; init; } = true;

    public long SizeInBytes { get; }

    public DateTimeOffset? CreatedAt { get; }

    public DateTimeOffset? UpdatedAt { get; }

    public DateTimeOffset? AccessedAt { get; }

    public static ServiceBusEntityRuntime Empty { get; } = new(ServiceBusMessageCounts.Empty);
}
