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

    /// <summary>The service's own ranges, so a value it would refuse or change is refused before anything is saved.</summary>
    public QueueSettingLimits? Limits { get; init; }
}

/// <summary>
/// The ranges a service accepts for each setting; null means the service sets no limit QueueLoom knows of.
/// <see cref="Check"/> names the service and its limit, so the person sees why a value cannot be saved.
/// </summary>
public sealed record QueueSettingLimits(string ServiceName)
{
    public TimeSpan? MinTimeToLive { get; init; }
    public TimeSpan? MaxTimeToLive { get; init; }
    public string TimeToLiveName { get; init; } = "time to live";
    public int? MinDeliveryCount { get; init; }
    public int? MaxDeliveryCount { get; init; }
    public string DeliveryCountName { get; init; } = "maximum delivery count";
    public TimeSpan? MinLock { get; init; }
    public TimeSpan? MaxLock { get; init; }
    /// <summary>The lock must be longer than zero (used when the service documents no other lower bound).</summary>
    public bool LockMustBePositive { get; init; }
    public string LockName { get; init; } = "lock duration";

    /// <summary>The first setting outside the service's range, described for the person; null when all fit.</summary>
    public string? Check(QueueSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);
        if (settings.MessageTimeToLive is { } ttl && (ttl < MinTimeToLive || ttl > MaxTimeToLive))
        {
            return $"{ServiceName} accepts a {TimeToLiveName} of {Range(MinTimeToLive, MaxTimeToLive)}.";
        }
        if (settings.MaxDeliveryCount is { } count && (count < MinDeliveryCount || count > MaxDeliveryCount))
        {
            return $"{ServiceName} accepts a {DeliveryCountName} of {Range(MinDeliveryCount, MaxDeliveryCount)}.";
        }
        if (settings.LockDuration is { } lockDuration &&
            (lockDuration < MinLock || lockDuration > MaxLock || (LockMustBePositive && lockDuration <= TimeSpan.Zero)))
        {
            return MinLock is null && LockMustBePositive && MaxLock is { } max
                ? $"{ServiceName} accepts a {LockName} of more than 0 seconds and at most {Describe(max)}."
                : $"{ServiceName} accepts a {LockName} of {Range(MinLock, MaxLock)}.";
        }
        return null;
    }

    private static string Range(TimeSpan? min, TimeSpan? max) =>
        min is { } low && max is { } high ? $"{Describe(low)} to {Describe(high)}"
        : min is { } onlyLow ? $"at least {Describe(onlyLow)}"
        : max is { } onlyHigh ? $"at most {Describe(onlyHigh)}"
        : "any value";

    private static string Range(int? min, int? max) =>
        min is { } low && max is { } high ? $"{Number(low)} to {Number(high)}"
        : min is { } onlyLow ? $"at least {Number(onlyLow)}"
        : max is { } onlyHigh ? $"at most {Number(onlyHigh)}"
        : "any value";

    private static string Number(int value) => value.ToString("N0", System.Globalization.CultureInfo.InvariantCulture);

    private static string Describe(TimeSpan value) =>
        value.TotalDays >= 1 && value.TotalDays % 1 == 0 ? Unit(value.TotalDays, "day")
        : value.TotalHours >= 1 && value.TotalHours % 1 == 0 ? Unit(value.TotalHours, "hour")
        : value.TotalMinutes >= 1 && value.TotalMinutes % 1 == 0 ? Unit(value.TotalMinutes, "minute")
        : Unit(value.TotalSeconds, "second");

    private static string Unit(double amount, string unit) =>
        $"{amount.ToString("0.##", System.Globalization.CultureInfo.InvariantCulture)} {unit}{(amount == 1 ? string.Empty : "s")}";
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
