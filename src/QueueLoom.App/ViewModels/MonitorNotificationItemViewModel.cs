using System.Globalization;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

public sealed class MonitorNotificationItemViewModel(
    string key,
    string environmentName,
    ServiceBusEntityReference source,
    string subQueueLabel,
    long count,
    DateTimeOffset firstDetectedAt) : ObservableObject
{
    private long _count = count;
    private QueueLoom.Core.Monitoring.DeadLetterCountQuality _countQuality;
    private DateTimeOffset _lastDetectedAt = firstDetectedAt;

    public string Key { get; } = key;
    public string EnvironmentName { get; } = environmentName;
    public ServiceBusEntityReference Source { get; } = source;
    public string SourceName => Source.DisplayName;
    public string EntityName => Source.Name;
    public string ParentTopicName => Source.TopicName ?? string.Empty;
    public bool IsQueue => Source.Kind == ServiceBusEntityKind.Queue;
    public bool IsSubscription => Source.Kind == ServiceBusEntityKind.Subscription;
    public string SubQueueLabel { get; } = subQueueLabel;
    public DateTimeOffset FirstDetectedAt { get; } = firstDetectedAt;

    public long Count
    {
        get => _count;
        set
        {
            if (SetProperty(ref _count, value)) OnPropertyChanged(nameof(CountText));
        }
    }

    /// <summary>How far the shown count can be trusted (a sample is a lower bound; SQS and Cloud Monitoring estimate).</summary>
    public QueueLoom.Core.Monitoring.DeadLetterCountQuality CountQuality
    {
        get => _countQuality;
        set
        {
            if (SetProperty(ref _countQuality, value))
            {
                OnPropertyChanged(nameof(CountText));
                OnPropertyChanged(nameof(CountIsLowerBound));
            }
        }
    }

    /// <summary>When the shown count was true, when the service says (a Cloud Monitoring point); older counts never overrule it.</summary>
    public DateTimeOffset? LastMeasuredAt { get; set; }

    /// <summary>What the count was taken from, when not the source itself (a Pub/Sub reader subscription).</summary>
    public string? MeasuredFrom { get; set; }

    /// <summary>Approximate zeros seen in a row: one alone does not resolve the notification.</summary>
    public int UnconfirmedClearChecks { get; set; }

    /// <summary>The service time of the last approximate zero counted: the same point read twice confirms nothing.</summary>
    public DateTimeOffset? LastClearPointAt { get; set; }

    /// <summary>What the queue held at the last check (see DeadLetterEntitySnapshot.ContentMarkers), when it was looked at.</summary>
    public IReadOnlyCollection<string>? ContentMarkers { get; set; }

    public bool CountIsLowerBound => CountQuality == QueueLoom.Core.Monitoring.DeadLetterCountQuality.LowerBound;

    public string CountText => QueueLoom.Core.Monitoring.DeadLetterCountText.Format(Count, CountQuality);

    public DateTimeOffset LastDetectedAt
    {
        get => _lastDetectedAt;
        set
        {
            if (SetProperty(ref _lastDetectedAt, value))
            {
                OnPropertyChanged(nameof(LastDetectedText));
            }
        }
    }

    public string FirstDetectedText => FirstDetectedAt.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
    public string LastDetectedText => LastDetectedAt.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
}
