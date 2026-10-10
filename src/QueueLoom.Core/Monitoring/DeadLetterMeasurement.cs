namespace QueueLoom.Core.Monitoring;

/// <summary>A dead-letter count and whether it is only a lower bound (a sampled queue).</summary>
public readonly record struct DeadLetterMeasurement(long Count, bool IsLowerBound)
{
    public static DeadLetterMeasurement Of(DeadLetterEntitySnapshot entity) =>
        new(entity.Count ?? 0, entity.CountIsLowerBound);

    /// <summary>
    /// How much the count grew, when that is proven: between two exact counts (exact), or a lower bound above an exact
    /// earlier count (at least that much, so the growth is itself a lower bound). Null when the two cannot be compared
    /// (an earlier lower bound says nothing of how much was there).
    /// </summary>
    public static DeadLetterMeasurement? ProvenIncrease(DeadLetterMeasurement? before, DeadLetterMeasurement now) =>
        before is not { } earlier ? null
        : earlier.IsLowerBound ? null
        : now.IsLowerBound ? (now.Count > earlier.Count ? new DeadLetterMeasurement(now.Count - earlier.Count, true) : null)
        : new DeadLetterMeasurement(now.Count - earlier.Count, false);

    public override string ToString() => DeadLetterCountText.Format(Count, IsLowerBound);
}
