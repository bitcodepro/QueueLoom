using System.Globalization;
using QueueLoom.App.Models;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

public sealed record ActivityItemViewModel(
    DateTimeOffset Timestamp,
    string Level,
    string Action,
    string Details,
    ServiceBusEntityReference? Source = null)
{
    public string Time => Timestamp.ToLocalTime().ToString("HH:mm:ss", CultureInfo.InvariantCulture);

    public Tone LevelTone => Tones.ForActivityLevel(Level);

    public string EntityName => Source?.Name ?? string.Empty;

    public string ParentTopicName => Source?.TopicName ?? string.Empty;

    public bool IsQueue => Source?.Kind == ServiceBusEntityKind.Queue;

    public bool IsTopic => Source?.Kind == ServiceBusEntityKind.Topic;

    public bool IsSubscription => Source?.Kind == ServiceBusEntityKind.Subscription;

    public bool HasSource => Source is not null;
}
