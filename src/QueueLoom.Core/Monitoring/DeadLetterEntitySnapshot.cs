using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Monitoring;

public sealed record DeadLetterEntitySnapshot
{
    public DeadLetterEntitySnapshot(
        ServiceBusEntityReference entity,
        long? count,
        long? previousCount = null,
        string? error = null,
        ServiceBusSubQueue subQueue = ServiceBusSubQueue.DeadLetter)
    {
        ArgumentNullException.ThrowIfNull(entity);

        if (!entity.CanBrowse)
        {
            throw new ArgumentException(
                "Dead-letter counts only apply to queues or subscriptions.",
                nameof(entity));
        }

        if (subQueue is not (ServiceBusSubQueue.DeadLetter or ServiceBusSubQueue.TransferDeadLetter))
        {
            throw new ArgumentException("A DLQ snapshot must target a dead-letter subqueue.", nameof(subQueue));
        }

        if (count is < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }

        if (previousCount is < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(previousCount));
        }

        Entity = entity;
        SubQueue = subQueue;
        Count = count;
        PreviousCount = previousCount;
        Error = error;
    }

    public ServiceBusEntityReference Entity { get; }

    public ServiceBusSubQueue SubQueue { get; }

    public long? Count { get; }

    public long? PreviousCount { get; }

    public string? Error { get; }

    public bool IsSuccessful => Count.HasValue && string.IsNullOrWhiteSpace(Error);

    public long? Change => Count.HasValue && PreviousCount.HasValue
        ? Count.Value - PreviousCount.Value
        : null;
}
