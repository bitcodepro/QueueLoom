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

    public string? CountNote => DeadLetterCountText.Note(Snapshot.CountQuality);

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
