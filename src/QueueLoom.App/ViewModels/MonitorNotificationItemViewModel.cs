namespace QueueLoom.App.ViewModels;

public sealed class MonitorNotificationItemViewModel(
    string key,
    string environmentName,
    string sourceName,
    string subQueueLabel,
    long count,
    DateTimeOffset firstDetectedAt) : ObservableObject
{
    private long _count = count;
    private DateTimeOffset _lastDetectedAt = firstDetectedAt;

    public string Key { get; } = key;
    public string EnvironmentName { get; } = environmentName;
    public string SourceName { get; } = sourceName;
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
