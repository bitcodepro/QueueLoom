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
    /// How much the count grew, when that can be said:
    /// <list type="bullet">
    /// <item>after an earlier lower bound, never (it says nothing of how much was there): null;</item>
    /// <item>a lower bound above an earlier count: at least the difference, itself a lower bound;</item>
    /// <item>otherwise the difference, exact between exact counts and estimated when either count is an estimate.</item>
    /// </list>
    /// </summary>
    public static DeadLetterMeasurement? ProvenIncrease(DeadLetterMeasurement? before, DeadLetterMeasurement now) =>
        before is not { } earlier ? null
        : earlier.IsLowerBound ? null
        : now.IsLowerBound ? (now.Count > earlier.Count ? new DeadLetterMeasurement(now.Count - earlier.Count, DeadLetterCountQuality.LowerBound) : null)
        : new DeadLetterMeasurement(now.Count - earlier.Count, DeadLetterCountQualities.Combine(earlier.Quality, now.Quality));

    public override string ToString() => DeadLetterCountText.Format(Count, Quality);
}
