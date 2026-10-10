namespace QueueLoom.Core.Monitoring;

public sealed record DeadLetterSnapshot
{
    public DeadLetterSnapshot(
        Guid profileId,
        DateTimeOffset capturedAt,
        IEnumerable<DeadLetterEntitySnapshot>? entities = null)
    {
        if (profileId == Guid.Empty)
        {
            throw new ArgumentException("The profile identifier must not be empty.", nameof(profileId));
        }

        ProfileId = profileId;
        CapturedAt = capturedAt;
        Entities = Array.AsReadOnly((entities ?? []).ToArray());
    }

    public Guid ProfileId { get; }

    public DateTimeOffset CapturedAt { get; }

    public IReadOnlyList<DeadLetterEntitySnapshot> Entities { get; }

    public long TotalCount => checked(Entities.Where(entity => entity.Count.HasValue).Sum(entity => entity.Count!.Value));

    /// <summary>The total includes a count that is only a lower bound, so the real total may be larger.</summary>
    public bool TotalIsLowerBound => TotalQuality == DeadLetterCountQuality.LowerBound;

    /// <summary>
    /// What the total is worth, unreadable sources included: exact counts with a failed one make a lower bound (the known
    /// part is a floor), and every source failing makes it unknown.
    /// </summary>
    public DeadLetterCountQuality TotalQuality => DeadLetterCountQualities.Combine(Entities.Select(entity => entity.CountQuality));

    public bool HasFailures => Entities.Any(entity => !entity.IsSuccessful);

    public bool HasDeadLetters => Entities.Any(entity => entity.Count > 0);
}
