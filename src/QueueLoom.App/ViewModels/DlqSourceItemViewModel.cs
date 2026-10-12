using QueueLoom.App.Models;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

public sealed class DlqSourceItemViewModel(
    Guid profileId,
    string profileName,
    string environmentLabel,
    Tone environmentTone,
    DeadLetterEntitySnapshot snapshot,
    string queueKindName = "queue")
{
    /// <summary>"QUEUE", or "TOPIC" for Kafka, whose topics hold the messages.</summary>
    public string QueueKindLabel { get; } = queueKindName.ToUpperInvariant();

    public Guid ProfileId { get; } = profileId;

    public string ProfileName { get; } = profileName;

    public string EnvironmentLabel { get; } = environmentLabel;

    public Tone EnvironmentTone { get; } = environmentTone;

    public DeadLetterEntitySnapshot Snapshot { get; } = snapshot;

    public ServiceBusEntityReference Entity => Snapshot.Entity;

    public string EntityPath => Entity.DisplayName;

    public string ParentTopicName => Entity.TopicName ?? string.Empty;

    public string EntityName => Entity.Name;

    public bool IsQueue => Entity.Kind == ServiceBusEntityKind.Queue;

    public bool IsSubscription => Entity.Kind == ServiceBusEntityKind.Subscription;

    public string EntityKind => IsQueue ? QueueKindLabel[..1] + QueueKindLabel[1..].ToLowerInvariant() : "Subscription";

    public string SubQueueLabel => Snapshot.SubQueue == ServiceBusSubQueue.TransferDeadLetter
        ? "TRANSFER DLQ"
        : "DLQ";

    public long Count => Snapshot.Count ?? 0;

    public bool CountIsLowerBound => Snapshot.CountIsLowerBound;

    /// <summary>Messages are there, or may be: a sampled queue that showed nothing can still be opened to look.</summary>
    public bool CanBrowse => Count > 0 || Snapshot.CountIsLowerBound;

    public DeadLetterCountQuality CountQuality => Snapshot.CountQuality;

    public string CountText => DeadLetterCountText.Format(Snapshot.Count ?? 0, Snapshot.CountQuality);

    /// <summary>Other sources whose dead letters go to the same destination; its count is theirs as much as this one's.</summary>
    public IReadOnlyList<string>? SharedWith { get; init; }

    public bool IsShared => SharedWith is { Count: > 0 };

    public string? CountNote => IsShared
        ? $"The whole count of the dead-letter queue {DeadLetterHistorySample.SharedName(Snapshot.MeasuredFrom!)}, shared with " +
          $"{string.Join(", ", SharedWith!)}: counted once in totals, not this source's own." +
          (DeadLetterCountText.Note(Snapshot.CountQuality) is { } note ? $" {note}" : string.Empty)
        : DeadLetterCountText.Note(Snapshot.CountQuality);

    /// <summary>Rows counted once per dead-letter destination: shared destinations are listed under each source.</summary>
    public static long CountOnce(IEnumerable<DlqSourceItemViewModel> rows) =>
        DeadLetterSnapshot.CountOnce(rows, row => $"{row.ProfileId:N}|{row.Snapshot.PhysicalKey}",
            row => row.Snapshot.IsSuccessful && row.Snapshot.Count.HasValue).Sum(row => row.Count);

    public string Delta => Snapshot.CountIsLowerBound || Snapshot.PreviousIsLowerBound ? "unknown" : Snapshot.Change switch
    {
        0 => Snapshot.ChangeQuality == DeadLetterCountQuality.Estimated ? "\u2248\u00b10" : "\u00b10",
        { } change => DeadLetterCountText.FormatChange(change, Snapshot.ChangeQuality),
        // A previous count that cannot be compared (another reader or dead-letter queue, an unknown count) is not "new".
        _ => Snapshot.PreviousCount is null ? "new" : "unknown"
    };

    public string Error => Snapshot.Error ?? string.Empty;

    public bool HasError => !Snapshot.IsSuccessful;
}
