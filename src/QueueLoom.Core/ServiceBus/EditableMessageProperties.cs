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

    /// <summary>
    /// The AMQP timestamp the publisher set (RabbitMQ). It is the producer's data, which consumers may use for ordering
    /// or staleness checks, so a resent or moved message keeps it; a new message gets the current time. Null for every
    /// other source, and for a RabbitMQ message published without one.
    /// </summary>
    [System.Text.Json.Serialization.JsonIgnore(Condition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull)]
    public DateTimeOffset? AmqpTimestamp { get; init; }

    /// <summary>
    /// The AMQP delivery mode of a RabbitMQ message: true for persistent (2), false for transient (1). A resent or moved
    /// message keeps it; a new message, or one that did not say, is persistent.
    /// </summary>
    [System.Text.Json.Serialization.JsonIgnore(Condition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull)]
    public bool? AmqpPersistent { get; init; }
}
