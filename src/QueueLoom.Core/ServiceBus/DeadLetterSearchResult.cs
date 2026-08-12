namespace QueueLoom.Core.ServiceBus;

public sealed record DeadLetterSearchSourceResult(
    ServiceBusEntityReference Source,
    ServiceBusSubQueue SubQueue,
    int ScannedMessageCount,
    IReadOnlyList<BrowsedMessage> Matches,
    bool ScanLimitReached = false,
    string? Error = null)
{
    public bool IsSuccessful => string.IsNullOrWhiteSpace(Error);
}

public sealed record DeadLetterSearchResult
{
    public DeadLetterSearchResult(
        Guid profileId,
        DateTimeOffset startedAt,
        DateTimeOffset completedAt,
        IEnumerable<DeadLetterSearchSourceResult> sources,
        bool resultLimitReached = false)
    {
        ProfileId = profileId;
        StartedAt = startedAt;
        CompletedAt = completedAt;
        Sources = Array.AsReadOnly(sources.ToArray());
        ResultLimitReached = resultLimitReached;
    }

    public Guid ProfileId { get; }

    public DateTimeOffset StartedAt { get; }

    public DateTimeOffset CompletedAt { get; }

    public IReadOnlyList<DeadLetterSearchSourceResult> Sources { get; }

    public int ScannedMessageCount => Sources.Sum(source => source.ScannedMessageCount);

    public int MatchCount => Sources.Sum(source => source.Matches.Count);

    public bool ResultLimitReached { get; }

    public bool ScanLimitReached => Sources.Any(source => source.ScanLimitReached);

    public bool HasFailures => Sources.Any(source => !source.IsSuccessful);

    public bool IsComplete => !ResultLimitReached && !ScanLimitReached && !HasFailures;

    public IReadOnlyList<BrowsedMessage> Matches => Array.AsReadOnly(Sources
        .SelectMany(source => source.Matches)
        .OrderBy(message => message.EnqueuedAt ?? DateTimeOffset.MinValue)
        .ThenBy(message => message.SequenceNumber)
        .ToArray());
}
