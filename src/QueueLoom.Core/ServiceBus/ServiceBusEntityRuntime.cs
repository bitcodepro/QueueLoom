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
