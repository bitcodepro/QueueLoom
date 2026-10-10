namespace QueueLoom.Core.Monitoring;

/// <summary>A dead-letter count, how far it can be trusted, and (where the service says) when and through what it was taken.</summary>
public readonly record struct DeadLetterMeasurement(long Count, DeadLetterCountQuality Quality)
{
    public DeadLetterMeasurement(long count, bool isLowerBound)
        : this(count, isLowerBound ? DeadLetterCountQuality.LowerBound : DeadLetterCountQuality.Exact)
    {
    }

    /// <summary>When the count was true, when older than the read (a Cloud Monitoring point); null when it is the read itself.</summary>
    public DateTimeOffset? MeasuredAt { get; init; }

    /// <summary>What was counted, when not the source itself: a Pub/Sub reader subscription, an SQS dead-letter queue.</summary>
    public string? MeasuredFrom { get; init; }

    public bool IsLowerBound => Quality == DeadLetterCountQuality.LowerBound;

    public static DeadLetterMeasurement Of(DeadLetterEntitySnapshot entity) =>
        new(entity.Count ?? 0, entity.CountQuality) { MeasuredAt = entity.MeasuredAt, MeasuredFrom = entity.MeasuredFrom };

    /// <summary>Two counts of different things (another reader, another dead-letter queue) are never compared.</summary>
    public static bool SameTarget(DeadLetterMeasurement first, DeadLetterMeasurement second) =>
        first.MeasuredFrom is null || second.MeasuredFrom is null || string.Equals(first.MeasuredFrom, second.MeasuredFrom, StringComparison.Ordinal);

    /// <summary>The later observation is older than the earlier one (a delayed point after a fresh read).</summary>
    public static bool IsOlder(DeadLetterMeasurement later, DeadLetterMeasurement earlier) =>
        later.MeasuredAt is { } at && earlier.MeasuredAt is { } before && at < before;

    /// <summary>
    /// The signed change from <paramref name="before"/> to <paramref name="now"/>, when two counts allow one: exact
    /// between exact counts, estimated between exact or estimated ones, of the same target. Null otherwise: a lower
    /// bound, an unqualified or unknown count, or a count of something else says nothing of how much the queue changed.
    /// </summary>
    public static DeadLetterMeasurement? Difference(DeadLetterMeasurement? before, DeadLetterMeasurement now) =>
        before is { } earlier && Comparable(earlier.Quality) && Comparable(now.Quality) && SameTarget(earlier, now)
            ? new DeadLetterMeasurement(now.Count - earlier.Count, DeadLetterCountQualities.Combine(earlier.Quality, now.Quality))
            : null;

    /// <summary>
    /// How much the count grew, when that can be said: a <see cref="Difference"/>, or, after an exact count of the same
    /// target, a lower bound above it (at least the difference, itself a lower bound). After an estimate a lower bound
    /// proves nothing: estimated 60 then at least 80 is no growth if the queue really held 200.
    /// </summary>
    public static DeadLetterMeasurement? ProvenIncrease(DeadLetterMeasurement? before, DeadLetterMeasurement now) =>
        before is { Quality: DeadLetterCountQuality.Exact } exact && now.IsLowerBound
            ? now.Count > exact.Count && SameTarget(exact, now) ? new DeadLetterMeasurement(now.Count - exact.Count, DeadLetterCountQuality.LowerBound) : null
            : Difference(before, now);

    private static bool Comparable(DeadLetterCountQuality quality) =>
        quality is DeadLetterCountQuality.Exact or DeadLetterCountQuality.Estimated;

    public override string ToString() => DeadLetterCountText.Format(Count, Quality);
}
