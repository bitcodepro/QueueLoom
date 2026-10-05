namespace QueueLoom.Core.ServiceBus;

/// <summary>A message kept for a later resend: which original it was (for a move) and what is sent.</summary>
public sealed record ScheduledResendItem(
    ServiceBusEntityReference Source,
    ServiceBusSubQueue SubQueue,
    long SequenceNumber,
    string? MessageId,
    ServiceBusEntityReference Destination,
    MessageDraft Message)
{
    public static ScheduledResendItem From(ResendItem item) => new(
        item.Original.Source, item.Original.SubQueue, item.Original.SequenceNumber, item.Original.Properties.MessageId,
        item.Destination, item.Message);

    /// <summary>
    /// The resend item again. The original is rebuilt from its identity only: that is all a move needs to find it
    /// and remove it, and the removal still backs up what it finds at that time.
    /// </summary>
    public ResendItem ToResendItem() => new(
        new BrowsedMessage(Source, SubQueue, SequenceNumber, ReadOnlyMemory<byte>.Empty,
            new EditableMessageProperties(MessageId: MessageId)),
        Destination,
        Message);
}

/// <summary>
/// Resending that waits until <see cref="DueAt"/>. It runs while QueueLoom is open, connected to its environment
/// with write access on; until then it waits, and it can be run early or cancelled.
/// </summary>
public sealed record ScheduledResend(
    Guid Id,
    Guid ProfileId,
    string EnvironmentName,
    DateTimeOffset CreatedAt,
    DateTimeOffset DueAt,
    ResendMode Mode,
    int MessagesPerSecond,
    string DestinationDisplay,
    IReadOnlyList<ScheduledResendItem> Items)
{
    public const int MaximumPending = 50;
    public string? ConfigurationIdentity { get; init; }

    public static string IdentityFor(QueueLoom.Core.Profiles.ServiceBusProfile profile) =>
        Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(
            System.Text.Json.JsonSerializer.Serialize(profile with
            {
                Name = string.Empty,
                Environment = QueueLoom.Core.Profiles.EnvironmentKind.Development,
                CustomEnvironmentName = null,
                AccessMode = QueueLoom.Core.Profiles.ProfileAccessMode.ReadOnly,
                AllowQueueManagement = false
            }))));

    public bool IsDue(DateTimeOffset now) => DueAt <= now;
}

public interface IScheduledResendStore
{
    IReadOnlyList<ScheduledResend> Load();

    void Save(IReadOnlyList<ScheduledResend> resends);

    /// <summary>Add to the current persisted list without overwriting another window's changes.</summary>
    void Add(ScheduledResend resend);

    /// <summary>Atomically consume this exact pending job, before sending or cancelling it.</summary>
    bool TryRemove(ScheduledResend expected);

    /// <summary>
    /// Once: where a damaged list of scheduled resends was set aside (its jobs will not run), or null. The window
    /// tells the operator, who would otherwise expect those resends to happen.
    /// </summary>
    string? TakeSetAsideFile() => null;
}
