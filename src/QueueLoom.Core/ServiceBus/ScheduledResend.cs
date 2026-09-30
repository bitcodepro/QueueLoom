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

    public bool IsDue(DateTimeOffset now) => DueAt <= now;
}

public interface IScheduledResendStore
{
    IReadOnlyList<ScheduledResend> Load();

    void Save(IReadOnlyList<ScheduledResend> resends);
}
