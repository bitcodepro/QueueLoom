namespace QueueLoom.Core.Monitoring;

/// <summary>A dead-letter count and whether it is only a lower bound (a sampled queue).</summary>
public readonly record struct DeadLetterMeasurement(long Count, bool IsLowerBound)
{
    public static DeadLetterMeasurement Of(DeadLetterEntitySnapshot entity) =>
        new(entity.Count ?? 0, entity.CountIsLowerBound);

    /// <summary>
    /// How much the count grew, when that is proven: both exact, or a lower bound already above an exact earlier count.
    /// Null when the two cannot be compared (an earlier lower bound says nothing of how much was there).
    /// </summary>
    public static long? ProvenIncrease(DeadLetterMeasurement? before, DeadLetterMeasurement now) =>
        before is not { } earlier ? null
        : earlier.IsLowerBound ? null
        : now.IsLowerBound ? (now.Count > earlier.Count ? now.Count - earlier.Count : null)
        : now.Count - earlier.Count;

    public override string ToString() => DeadLetterCountText.Format(Count, IsLowerBound);
}
