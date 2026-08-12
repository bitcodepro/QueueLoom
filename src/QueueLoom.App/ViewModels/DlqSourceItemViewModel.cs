using QueueLoom.Core.Monitoring;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

public sealed class DlqSourceItemViewModel(
    Guid profileId,
    string profileName,
    string environmentLabel,
    string environmentColor,
    DeadLetterEntitySnapshot snapshot)
{
    public Guid ProfileId { get; } = profileId;

    public string ProfileName { get; } = profileName;

    public string EnvironmentLabel { get; } = environmentLabel;

    public string EnvironmentColor { get; } = environmentColor;

    public DeadLetterEntitySnapshot Snapshot { get; } = snapshot;

    public ServiceBusEntityReference Entity => Snapshot.Entity;

    public string EntityPath => Entity.DisplayName;

    public string ParentTopicName => Entity.TopicName ?? string.Empty;

    public string EntityName => Entity.Name;

    public bool IsQueue => Entity.Kind == ServiceBusEntityKind.Queue;

    public bool IsSubscription => Entity.Kind == ServiceBusEntityKind.Subscription;

    public string EntityKind => IsQueue ? "Queue" : "Subscription";

    public string SubQueueLabel => Snapshot.SubQueue == ServiceBusSubQueue.TransferDeadLetter
        ? "TRANSFER DLQ"
        : "DLQ";

    public long Count => Snapshot.Count ?? 0;

    public string Delta => Snapshot.Change switch
    {
        > 0 => $"+{Snapshot.Change}",
        < 0 => Snapshot.Change.ToString()!,
        0 => "±0",
        _ => "new"
    };

    public string Error => Snapshot.Error ?? string.Empty;

    public bool HasError => !Snapshot.IsSuccessful;
}
