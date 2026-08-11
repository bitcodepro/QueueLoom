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

    public long SizeInBytes { get; }

    public DateTimeOffset? CreatedAt { get; }

    public DateTimeOffset? UpdatedAt { get; }

    public DateTimeOffset? AccessedAt { get; }

    public static ServiceBusEntityRuntime Empty { get; } = new(ServiceBusMessageCounts.Empty);
}
