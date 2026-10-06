namespace QueueLoom.Core.ServiceBus;

/// <summary>Identifies one dead-lettered message chosen by the operator (for example from a search result).</summary>
/// <remarks>
/// The sequence number is unique within an entity and survives dead-lettering. The message ID, when present,
/// is checked as well so a message is only deleted if it is still the one the operator reviewed.
/// </remarks>
public sealed record DeadLetterMessageKey(
    ServiceBusEntityReference Source,
    ServiceBusSubQueue SubQueue,
    long SequenceNumber,
    string? MessageId = null)
{
    public bool IsValid => Source.CanBrowse &&
                           SubQueue is ServiceBusSubQueue.DeadLetter or ServiceBusSubQueue.TransferDeadLetter &&
                           SequenceNumber >= 0;
}

public sealed record DeleteDeadLetterMessagesRequest
{
    public const int MaximumMessages = 1_000;
    public const int DefaultMaximumScannedPerSubQueue = 5_000;
    public const int DefaultBatchSize = 20;

    public DeleteDeadLetterMessagesRequest(
        IEnumerable<DeadLetterMessageKey> messages,
        int maximumScannedPerSubQueue = DefaultMaximumScannedPerSubQueue,
        int batchSize = DefaultBatchSize)
    {
        ArgumentNullException.ThrowIfNull(messages);
        var keys = messages
            .DistinctBy(message => (message.Source, message.SubQueue, message.SequenceNumber))
            .ToArray();
        if (keys.Length == 0)
        {
            throw new ArgumentException("Select at least one dead-letter message.", nameof(messages));
        }
        if (keys.Length > MaximumMessages)
        {
            throw new ArgumentException(
                $"At most {MaximumMessages:N0} messages can be deleted at once. Narrow the selection.",
                nameof(messages));
        }
        if (keys.Any(key => !key.IsValid))
        {
            throw new ArgumentException(
                "Only messages in a queue or subscription dead-letter queue can be deleted.",
                nameof(messages));
        }
        if (maximumScannedPerSubQueue is < 1 or > 100_000)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maximumScannedPerSubQueue),
                "The scan limit must be between 1 and 100,000 messages.");
        }
        if (batchSize is < 1 or > 100)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), "Batch size must be between 1 and 100.");
        }

        Messages = Array.AsReadOnly(keys);
        MaximumScannedPerSubQueue = maximumScannedPerSubQueue;
        BatchSize = batchSize;
    }

    public IReadOnlyList<DeadLetterMessageKey> Messages { get; }

    /// <summary>Upper bound of messages received (and briefly locked) per dead-letter queue while looking for the selection.</summary>
    public int MaximumScannedPerSubQueue { get; }

    public int BatchSize { get; }

    public IEnumerable<IGrouping<(ServiceBusEntityReference Source, ServiceBusSubQueue SubQueue), DeadLetterMessageKey>> BySubQueue =>
        Messages.GroupBy(message => (message.Source, message.SubQueue));
}

/// <summary>Progress within one dead-letter queue: messages looked at so far and messages deleted.</summary>
public sealed record DeadLetterMessageDeletionProgress(
    ServiceBusEntityReference Source,
    ServiceBusSubQueue SubQueue,
    int QueueNumber,
    int QueueCount,
    int ScannedCount,
    int DeletedCount);

public enum DeadLetterMessageDeletionOutcome
{
    /// <summary>Backed up and removed from the dead-letter queue.</summary>
    Deleted,

    /// <summary>Not present any more (already removed or resubmitted) or not reached within the scan limit.</summary>
    NotFound,

    /// <summary>The backup or the settlement failed; the message may still be in the queue.</summary>
    Failed,

    /// <summary>The operation was cancelled before this message was processed.</summary>
    Cancelled
}

public sealed record DeadLetterMessageDeletionResult(
    DeadLetterMessageKey Message,
    DeadLetterMessageDeletionOutcome Outcome,
    string? Detail = null);

public sealed record DeleteDeadLetterMessagesResult
{
    public DeleteDeadLetterMessagesResult(
        Guid profileId,
        DateTimeOffset startedAt,
        DateTimeOffset completedAt,
        IEnumerable<DeadLetterMessageDeletionResult> messages,
        string backupDirectory)
    {
        if (profileId == Guid.Empty)
        {
            throw new ArgumentException("The profile identifier must not be empty.", nameof(profileId));
        }
        ProfileId = profileId;
        StartedAt = startedAt;
        CompletedAt = completedAt;
        Messages = Array.AsReadOnly((messages ?? throw new ArgumentNullException(nameof(messages))).ToArray());
        BackupDirectory = Path.GetFullPath(
            string.IsNullOrWhiteSpace(backupDirectory)
                ? throw new ArgumentException("A backup directory is required.", nameof(backupDirectory))
                : backupDirectory);
    }

    public Guid ProfileId { get; }
    public DateTimeOffset StartedAt { get; }
    public DateTimeOffset CompletedAt { get; }
    public IReadOnlyList<DeadLetterMessageDeletionResult> Messages { get; }
    public string BackupDirectory { get; }

    /// <summary>Persistence or lock-release problems that do not change confirmed message outcomes.</summary>
    public IReadOnlyList<string> Warnings { get; init; } = [];

    public int DeletedCount => Count(DeadLetterMessageDeletionOutcome.Deleted);
    public int NotFoundCount => Count(DeadLetterMessageDeletionOutcome.NotFound);
    public int FailedCount => Count(DeadLetterMessageDeletionOutcome.Failed);
    public int CancelledCount => Count(DeadLetterMessageDeletionOutcome.Cancelled);

    private int Count(DeadLetterMessageDeletionOutcome outcome) =>
        Messages.Count(message => message.Outcome == outcome);
}
