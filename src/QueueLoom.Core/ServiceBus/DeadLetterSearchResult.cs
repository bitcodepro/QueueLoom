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
    public static string? RegexTimeoutError(int undecidedMessages) => UndecidedError(undecidedMessages, 0);

    /// <summary>
    /// The error of a source with messages the search could not decide: the regular expression ran out of time on
    /// some, and the decoded body of others is longer than QueueLoom reads. Null when there are none.
    /// </summary>
    public static string? UndecidedError(int regexTimeouts, int incompleteBodies)
    {
        var parts = new List<string>(2);
        if (regexTimeouts > 0)
        {
            parts.Add($"{regexTimeouts:N0} message(s) could not be checked: the regular expression took too long on them. " +
                      "Simplify the expression to search them.");
        }
        if (incompleteBodies > 0)
        {
            parts.Add($"{incompleteBodies:N0} message(s) could not be checked: their decoded body is longer than " +
                      $"{BodyDecoder.MaximumDecodedBytes / (1024 * 1024)} MiB (or {BodyDecoder.MaximumAvroRecords:N0} Avro records), " +
                      "and the part read does not decide the search.");
        }
        return parts.Count == 0 ? null : string.Join(" ", parts);
    }
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
