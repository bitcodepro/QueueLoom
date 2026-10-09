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
    DateTimeOffset? ScheduledEnqueueTime = null,
    string? AmqpType = null,
    string? AmqpAppId = null,
    string? AmqpContentEncoding = null,
    byte? AmqpPriority = null)
{
    public static EditableMessageProperties Empty { get; } = new();

    /// <summary>
    /// The SNS Publish Subject read from an SNS notification envelope. It is published again as the native Subject
    /// only, never as a "Subject" message attribute the original did not have (SNS drops a message with more than ten
    /// attributes for raw SQS subscriptions). Null for every other source.
    /// </summary>
    [System.Text.Json.Serialization.JsonIgnore(Condition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull)]
    public string? NativeSubject { get; init; }
}
