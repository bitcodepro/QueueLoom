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
        set => SetProperty(ref _count, value);
    }

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

    public string FirstDetectedText => FirstDetectedAt.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss");
    public string LastDetectedText => LastDetectedAt.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss");
}
