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

    /// <summary>
    /// Each dead-letter destination once: sources that share one (an SQS dead-letter queue several queues redrive to,
    /// a Pub/Sub dead-letter topic read through one subscription) all report its whole count, which is one count.
    /// </summary>
    public IEnumerable<DeadLetterEntitySnapshot> CountedOnce => CountOnce(Entities, entity => entity.MeasuredFrom, entity => entity.Count.HasValue);

    /// <summary>
    /// Keeps one item per shared target (the first that has a count, else the first), and every item without a target.
    /// </summary>
    public static IEnumerable<T> CountOnce<T>(IEnumerable<T> items, Func<T, string?> target, Func<T, bool> hasCount) =>
        items.Select((item, index) => (Item: item, Key: target(item) is { } shared ? "@" + shared : "#" + index))
            .GroupBy(pair => pair.Key, StringComparer.Ordinal)
            .Select(group => group.Select(pair => pair.Item).FirstOrDefault(hasCount) ?? group.First().Item);

    /// <summary>The sources that share a dead-letter destination with another, by that destination.</summary>
    public IReadOnlyDictionary<string, IReadOnlyList<DeadLetterEntitySnapshot>> SharedTargets =>
        Entities.Where(entity => entity.MeasuredFrom is not null)
            .GroupBy(entity => entity.MeasuredFrom!, StringComparer.Ordinal)
            .Where(group => group.Count() > 1)
            .ToDictionary(group => group.Key, group => (IReadOnlyList<DeadLetterEntitySnapshot>)group.ToArray(), StringComparer.Ordinal);

    public long TotalCount => checked(CountedOnce.Where(entity => entity.Count.HasValue).Sum(entity => entity.Count!.Value));

    /// <summary>The total includes a count that is only a lower bound, so the real total may be larger.</summary>
    public bool TotalIsLowerBound => TotalQuality == DeadLetterCountQuality.LowerBound;

    /// <summary>
    /// What the total is worth, unreadable sources included: exact counts with a failed one make a lower bound (the known
    /// part is a floor), and every source failing makes it unknown.
    /// </summary>
    public DeadLetterCountQuality TotalQuality => DeadLetterCountQualities.Combine(CountedOnce.Select(entity => entity.CountQuality));

    public bool HasFailures => Entities.Any(entity => !entity.IsSuccessful);

    public bool HasDeadLetters => Entities.Any(entity => entity.Count > 0);
}
