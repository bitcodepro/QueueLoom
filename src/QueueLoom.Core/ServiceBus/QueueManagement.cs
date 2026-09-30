namespace QueueLoom.Core.ServiceBus;

/// <summary>
/// Settings QueueLoom can set on a queue (or Kafka topic). Each service supports a subset, described by
/// <see cref="QueueManagementCapabilities"/>; null means "not set" or "leave as it is".
/// </summary>
/// <param name="MessageTimeToLive">How long an unread message is kept (Service Bus TTL, SQS retention, Kafka retention.ms, RabbitMQ x-message-ttl).</param>
/// <param name="MaxDeliveryCount">Deliveries before a message is dead-lettered (Service Bus, SQS maxReceiveCount, RabbitMQ quorum delivery limit).</param>
/// <param name="LockDuration">How long a received message stays hidden from other receivers (Service Bus lock, SQS visibility timeout).</param>
/// <param name="DeadLetterOnExpiration">Move expired messages to the dead-letter queue (Service Bus).</param>
/// <param name="Partitions">Number of partitions (Kafka; can only grow).</param>
public sealed record QueueSettings(
    TimeSpan? MessageTimeToLive = null,
    int? MaxDeliveryCount = null,
    TimeSpan? LockDuration = null,
    bool? DeadLetterOnExpiration = null,
    int? Partitions = null);

/// <param name="CreateDeadLetterQueue">
/// Also create a dead-letter queue and connect it (SQS redrive policy, RabbitMQ dead-letter routing, Kafka ".DLT"
/// topic). Service Bus queues always have one.
/// </param>
/// <param name="TopicName">The topic a new subscription reads from (Pub/Sub, where subscriptions are what is managed).</param>
public sealed record QueueDefinition(string Name, QueueSettings Settings, bool CreateDeadLetterQueue = true, string? TopicName = null);

/// <summary>Which settings a service lets QueueLoom set when creating a queue, and which it can change later.</summary>
public sealed record QueueManagementCapabilities(
    string QueueKindName,
    QueueSettingFlags OnCreate,
    QueueSettingFlags OnUpdate,
    bool CanCreateDeadLetterQueue,
    string? UpdateNote = null)
{
    public bool CanUpdate => OnUpdate != QueueSettingFlags.None;

    /// <summary>
    /// The managed things are subscriptions of topics (Pub/Sub): a new one is created on the selected topic, and
    /// changing or deleting applies to the selected subscription.
    /// </summary>
    public bool ManagesSubscriptions { get; init; }
}

[Flags]
public enum QueueSettingFlags
{
    None = 0,
    MessageTimeToLive = 1,
    MaxDeliveryCount = 2,
    LockDuration = 4,
    DeadLetterOnExpiration = 8,
    Partitions = 16
}

public static class QueueNames
{
    /// <summary>
    /// A conservative rule every supported service accepts: letters, digits, '.', '-' and '_', starting with a
    /// letter or digit, at most 80 characters (SQS's limit).
    /// </summary>
    public static string? Validate(string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return "Enter a name.";
        }
        if (name.Length > 80)
        {
            return "Use at most 80 characters.";
        }
        if (!char.IsAsciiLetterOrDigit(name[0]) || name.Any(character => !(char.IsAsciiLetterOrDigit(character) || character is '.' or '-' or '_')))
        {
            return "Use letters, digits, '.', '-' and '_', starting with a letter or digit.";
        }
        return null;
    }
}
