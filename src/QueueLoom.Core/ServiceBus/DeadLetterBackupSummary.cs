namespace QueueLoom.Core.ServiceBus;

public sealed record DeadLetterBackupSummary(
    string FilePath,
    Guid ProfileId,
    string ProfileName,
    string Environment,
    string? FullyQualifiedNamespace,
    ServiceBusEntityReference Source,
    ServiceBusSubQueue SubQueue,
    long SequenceNumber,
    string? MessageId,
    string? CorrelationId,
    string? Subject,
    DateTimeOffset? EnqueuedAt,
    DateTimeOffset BackedUpAt,
    long BodySize,
    string? Error = null)
{
    public bool IsReadable => string.IsNullOrWhiteSpace(Error);
}
