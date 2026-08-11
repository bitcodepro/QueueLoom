namespace QueueLoom.Core.ServiceBus;

public sealed record EditableMessageProperties(
    string? MessageId = null,
    string? CorrelationId = null,
    string? ContentType = null,
    string? Subject = null,
    string? To = null,
    string? ReplyTo = null,
    string? SessionId = null,
    string? ReplyToSessionId = null,
    string? PartitionKey = null,
    string? TransactionPartitionKey = null,
    TimeSpan? TimeToLive = null,
    DateTimeOffset? ScheduledEnqueueTime = null)
{
    public static EditableMessageProperties Empty { get; } = new();
}
