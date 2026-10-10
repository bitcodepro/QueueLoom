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

    /// <summary>The weakest quality among the counted sources: the total is no better than its least certain part.</summary>
    public DeadLetterCountQuality TotalQuality =>
        DeadLetterCountQualities.Combine(Entities.Where(entity => entity.Count.HasValue).Select(entity => entity.CountQuality));

    public bool HasFailures => Entities.Any(entity => !entity.IsSuccessful);

    public bool HasDeadLetters => Entities.Any(entity => entity.Count > 0);
}
