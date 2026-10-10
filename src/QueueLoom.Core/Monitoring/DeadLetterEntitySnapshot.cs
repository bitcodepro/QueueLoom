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

    /// <summary>
    /// How far the count can be trusted: exact, estimated (SQS, Pub/Sub Cloud Monitoring), or only a lower bound
    /// (Pub/Sub counted by reading, where neither a smaller sample nor an empty one proves the size or an empty queue).
    /// </summary>
    public DeadLetterCountQuality CountQuality { get; init; }

    /// <summary>Shorthand for a lower-bound count; setting it true makes the count a lower bound.</summary>
    public bool CountIsLowerBound
    {
        get => CountQuality == DeadLetterCountQuality.LowerBound;
        init { if (value) CountQuality = DeadLetterCountQuality.LowerBound; }
    }

    /// <summary>How far the previous count could be trusted.</summary>
    public DeadLetterCountQuality PreviousQuality { get; init; }

    /// <summary>The previous count was only a lower bound, so no change can be computed from it.</summary>
    public bool PreviousIsLowerBound
    {
        get => PreviousQuality == DeadLetterCountQuality.LowerBound;
        init { if (value) PreviousQuality = DeadLetterCountQuality.LowerBound; }
    }

    public long? PreviousCount { get; }

    public string? Error { get; }

    public bool IsSuccessful => Count.HasValue && string.IsNullOrWhiteSpace(Error);

    /// <summary>Null when either count is unknown or only a lower bound; see <see cref="ChangeQuality"/> for an estimate.</summary>
    public long? Change => Count.HasValue && PreviousCount.HasValue && !CountIsLowerBound && !PreviousIsLowerBound
        ? Count.Value - PreviousCount.Value
        : null;

    /// <summary>Exact between exact counts, estimated when either is an estimate.</summary>
    public DeadLetterCountQuality ChangeQuality => DeadLetterCountQualities.Combine(CountQuality, PreviousQuality);
}
