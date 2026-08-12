namespace QueueLoom.Core.ServiceBus;

public sealed record DeadLetterPurgeSourceResult(
    ServiceBusEntityReference Source,
    ServiceBusSubQueue SubQueue,
    long DeletedCount,
    string? Error = null,
    bool LimitReached = false)
{
    public bool IsSuccessful => string.IsNullOrWhiteSpace(Error) && !LimitReached;
}

public sealed record DeadLetterPurgeResult
{
    public DeadLetterPurgeResult(
        Guid profileId,
        DateTimeOffset startedAt,
        DateTimeOffset completedAt,
        IEnumerable<DeadLetterPurgeSourceResult> sources,
        string backupDirectory)
    {
        if (profileId == Guid.Empty)
        {
            throw new ArgumentException("The profile identifier must not be empty.", nameof(profileId));
        }
        if (completedAt < startedAt)
        {
            throw new ArgumentException("Completion time cannot precede start time.", nameof(completedAt));
        }

        ProfileId = profileId;
        StartedAt = startedAt;
        CompletedAt = completedAt;
        Sources = Array.AsReadOnly((sources ?? throw new ArgumentNullException(nameof(sources))).ToArray());
        BackupDirectory = Path.GetFullPath(
            string.IsNullOrWhiteSpace(backupDirectory)
                ? throw new ArgumentException("A backup directory is required.", nameof(backupDirectory))
                : backupDirectory);
    }

    public Guid ProfileId { get; }

    public DateTimeOffset StartedAt { get; }

    public DateTimeOffset CompletedAt { get; }

    public IReadOnlyList<DeadLetterPurgeSourceResult> Sources { get; }

    public string BackupDirectory { get; }

    public long DeletedCount => checked(Sources.Sum(source => source.DeletedCount));

    public bool HasFailures => Sources.Any(source => !source.IsSuccessful);
}
