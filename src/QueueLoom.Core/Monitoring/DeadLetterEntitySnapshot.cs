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

    private readonly DeadLetterCountQuality _countQuality;

    /// <summary>
    /// How far the count can be trusted: exact, estimated (SQS, Pub/Sub Cloud Monitoring), or only a lower bound
    /// (Pub/Sub counted by reading, where neither a smaller sample nor an empty one proves the size or an empty queue).
    /// Unknown whenever there is no count.
    /// </summary>
    public DeadLetterCountQuality CountQuality
    {
        get => Count is null ? DeadLetterCountQuality.Unknown : _countQuality;
        init => _countQuality = value;
    }

    /// <summary>
    /// When the count was true, when that is older than the snapshot: a Cloud Monitoring point is a few minutes old. A
    /// newer observation of the same source is never overruled by an older one.
    /// </summary>
    public DateTimeOffset? MeasuredAt { get; init; }

    /// <summary>
    /// What was actually counted, when that is not the source itself (Pub/Sub counts a dead-letter topic through the
    /// subscription that reads it). Counts from different readers are never compared.
    /// </summary>
    public string? MeasuredFrom { get; init; }

    /// <summary>What the previous count was taken from; a count of another target has no change against this one.</summary>
    public string? PreviousMeasuredFrom { get; init; }

    /// <summary>The most messages whose identities are read to tell a replaced message from an unchanged queue.</summary>
    public const int ContentMarkerLimit = 100;

    /// <summary>
    /// What the queue holds, as opaque markers, where the service lets it be seen without side effects: Azure Service
    /// Bus's peeked sequence numbers and message IDs (up to <see cref="ContentMarkerLimit"/> messages), Kafka's partition
    /// end offsets. A marker not seen before means a new message arrived, even when the count stayed the same. Null
    /// when the contents were not looked at.
    /// </summary>
    public IReadOnlyCollection<string>? ContentMarkers { get; init; }

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

    /// <summary>
    /// The change since the previous count, when the two allow one (see <see cref="DeadLetterMeasurement.Difference"/>):
    /// null around a lower bound, an unqualified or an unknown count.
    /// </summary>
    public long? Change => Count.HasValue && PreviousCount.HasValue
        ? DeadLetterMeasurement.Difference(
            new DeadLetterMeasurement(PreviousCount.Value, PreviousQuality) { MeasuredFrom = PreviousMeasuredFrom },
            new DeadLetterMeasurement(Count.Value, CountQuality) { MeasuredFrom = MeasuredFrom })?.Count
        : null;

    /// <summary>Exact between exact counts, estimated when either is an estimate.</summary>
    public DeadLetterCountQuality ChangeQuality => DeadLetterCountQualities.Combine(CountQuality, PreviousQuality);
}
