namespace QueueLoom.Core.Monitoring;

/// <summary>A dead-letter count and how far it can be trusted.</summary>
public readonly record struct DeadLetterMeasurement(long Count, DeadLetterCountQuality Quality)
{
    public DeadLetterMeasurement(long count, bool isLowerBound)
        : this(count, isLowerBound ? DeadLetterCountQuality.LowerBound : DeadLetterCountQuality.Exact)
    {
    }

    public bool IsLowerBound => Quality == DeadLetterCountQuality.LowerBound;

    public static DeadLetterMeasurement Of(DeadLetterEntitySnapshot entity) => new(entity.Count ?? 0, entity.CountQuality);

    /// <summary>
    /// The signed change from <paramref name="before"/> to <paramref name="now"/>, when two counts allow one: exact
    /// between exact counts, estimated between exact or estimated ones. Null otherwise: a lower bound, an unqualified or
    /// an unknown count on either side says nothing of how much the queue really changed.
    /// </summary>
    public static DeadLetterMeasurement? Difference(DeadLetterMeasurement? before, DeadLetterMeasurement now) =>
        before is { } earlier && Comparable(earlier.Quality) && Comparable(now.Quality)
            ? new DeadLetterMeasurement(now.Count - earlier.Count, DeadLetterCountQualities.Combine(earlier.Quality, now.Quality))
            : null;

    /// <summary>
    /// How much the count grew, when that can be said: a <see cref="Difference"/>, or, after an exact count, a lower
    /// bound above it (at least the difference, itself a lower bound). After an estimate a lower bound proves nothing:
    /// estimated 60 then at least 80 is no growth if the queue really held 200.
    /// </summary>
    public static DeadLetterMeasurement? ProvenIncrease(DeadLetterMeasurement? before, DeadLetterMeasurement now) =>
        before is { Quality: DeadLetterCountQuality.Exact } exact && now.IsLowerBound
            ? now.Count > exact.Count ? new DeadLetterMeasurement(now.Count - exact.Count, DeadLetterCountQuality.LowerBound) : null
            : Difference(before, now);

    private static bool Comparable(DeadLetterCountQuality quality) =>
        quality is DeadLetterCountQuality.Exact or DeadLetterCountQuality.Estimated;

    public override string ToString() => DeadLetterCountText.Format(Count, Quality);
}
