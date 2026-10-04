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

    /// <summary>The error of a source in which the regular expression ran out of time on some messages, or null.</summary>
    public static string? RegexTimeoutError(int undecidedMessages) => undecidedMessages > 0
        ? $"{undecidedMessages:N0} message(s) could not be checked: the regular expression took too long on them. " +
          "Simplify the expression to search them."
        : null;
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
        // A derived sequence number (SQS, Pub/Sub, Kafka) is a hash: ties keep the order they were received in.
        .ThenBy(message => message.HasSequenceNumber ? message.SequenceNumber : 0)
        .ToArray());
}
