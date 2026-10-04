using System.Globalization;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

public enum TimeUnit
{
    Minutes,
    Hours,
    Days
}

/// <summary>
/// The new-queue and queue-settings dialog. It shows only the settings the connected service supports, in the
/// units people think in (minutes, hours, days), and leaves empty fields to the service's defaults.
/// </summary>
public sealed class QueueDialogViewModel : ObservableObject
{
    private readonly QueueManagementCapabilities _capabilities;
    private string _name = string.Empty;
    private double? _timeToLive;
    private TimeUnit _timeToLiveUnit = TimeUnit.Days;
    private int? _maxDeliveryCount;
    private int? _lockSeconds;
    private bool _deadLetterOnExpiration;
    private int? _partitions;
    private bool _createDeadLetterQueue = true;
    private string _error = string.Empty;

    /// <summary>Creates the dialog for a new queue (<paramref name="existing"/> null) or for changing one.</summary>
    public QueueDialogViewModel(QueueManagementCapabilities capabilities, string environmentName, string? existing = null,
        QueueSettings? current = null, string? topicName = null)
    {
        _capabilities = capabilities;
        TopicName = topicName;
        EnvironmentName = environmentName;
        IsNew = existing is null;
        _name = existing ?? string.Empty;
        if (current?.MessageTimeToLive is { } ttl && ttl < TimeSpan.FromDays(3650))
        {
            (_timeToLive, _timeToLiveUnit) = ttl.TotalDays >= 1 && ttl.TotalDays % 1 == 0 ? (ttl.TotalDays, TimeUnit.Days)
                : ttl.TotalHours >= 1 && ttl.TotalHours % 1 == 0 ? (ttl.TotalHours, TimeUnit.Hours)
                : (Math.Round(ttl.TotalMinutes, 2), TimeUnit.Minutes);
        }
        _maxDeliveryCount = current?.MaxDeliveryCount;
        _lockSeconds = current?.LockDuration is { } lockDuration ? (int)lockDuration.TotalSeconds : null;
        _deadLetterOnExpiration = current?.DeadLetterOnExpiration ?? false;
        _partitions = current?.Partitions ?? (IsNew && Shows(QueueSettingFlags.Partitions) ? 3 : null);
        CurrentPartitions = current?.Partitions;
    }

    public bool IsNew { get; }

    /// <summary>The topic a new Pub/Sub subscription reads from.</summary>
    public string? TopicName { get; }

    public bool HasTopicName => IsNew && !string.IsNullOrEmpty(TopicName);

    /// <summary>Pub/Sub calls the lock an acknowledgement deadline.</summary>
    public string LockLabel => _capabilities.ManagesSubscriptions ? "ACK DEADLINE · SECONDS" : "LOCK · SECONDS";

    public string EnvironmentName { get; }

    public string KindName => _capabilities.QueueKindName;

    public string Title => IsNew ? $"New {KindName}" : $"{char.ToUpperInvariant(KindName[0])}{KindName[1..]} settings · {Name}";

    public string ConfirmLabel => IsNew ? $"Create {KindName}" : "Save changes";

    public string? Note => IsNew ? null : _capabilities.UpdateNote;

    public bool HasNote => !string.IsNullOrEmpty(Note);

    public string Name
    {
        get => _name;
        set => SetProperty(ref _name, value ?? string.Empty);
    }

    public IReadOnlyList<TimeUnit> TimeUnits { get; } = Enum.GetValues<TimeUnit>();

    public double? TimeToLive
    {
        get => _timeToLive;
        set => SetProperty(ref _timeToLive, value);
    }

    public TimeUnit TimeToLiveUnit
    {
        get => _timeToLiveUnit;
        set => SetProperty(ref _timeToLiveUnit, value);
    }

    public int? MaxDeliveryCount
    {
        get => _maxDeliveryCount;
        set => SetProperty(ref _maxDeliveryCount, value);
    }

    public int? LockSeconds
    {
        get => _lockSeconds;
        set => SetProperty(ref _lockSeconds, value);
    }

    public bool DeadLetterOnExpiration
    {
        get => _deadLetterOnExpiration;
        set => SetProperty(ref _deadLetterOnExpiration, value);
    }

    public int? Partitions
    {
        get => _partitions;
        set => SetProperty(ref _partitions, value);
    }

    public int? CurrentPartitions { get; }

    public bool CreateDeadLetterQueue
    {
        get => _createDeadLetterQueue;
        set => SetProperty(ref _createDeadLetterQueue, value);
    }

    public bool ShowTimeToLive => Shows(QueueSettingFlags.MessageTimeToLive);

    public bool ShowMaxDeliveryCount => Shows(QueueSettingFlags.MaxDeliveryCount);

    public bool ShowLockDuration => Shows(QueueSettingFlags.LockDuration);

    public bool ShowDeliveryOrLock => ShowMaxDeliveryCount || ShowLockDuration;

    public bool ShowDeadLetterOnExpiration => Shows(QueueSettingFlags.DeadLetterOnExpiration);

    public bool ShowPartitions => Shows(QueueSettingFlags.Partitions);

    public bool ShowCreateDeadLetterQueue => IsNew && _capabilities.CanCreateDeadLetterQueue;

    public string DeadLetterQueueLabel => KindName switch
    {
        "topic" => $"Also create the dead-letter topic {DisplayName}.DLT",
        "subscription" => $"Also create the dead-letter topic {DisplayName}-dead-letter, with a subscription to read it",
        _ => "Also create a dead-letter queue and send failed messages there"
    };

    public string Error
    {
        get => _error;
        private set
        {
            if (SetProperty(ref _error, value))
            {
                OnPropertyChanged(nameof(HasError));
            }
        }
    }

    public bool HasError => Error.Length > 0;

    private string DisplayName => string.IsNullOrWhiteSpace(Name) ? "<name>" : Name.Trim();

    public QueueSettings? TryBuildSettings()
    {
        Error = string.Empty;
        if (IsNew && QueueNames.Validate(Name.Trim()) is { } nameError)
        {
            Error = nameError;
            return null;
        }
        if (TimeToLive is <= 0 || MaxDeliveryCount is < 1 or > 2000 || LockSeconds is < 0 or > 43_200 || Partitions is < 1 or > 10_000)
        {
            Error = "Use positive numbers: at most 2,000 deliveries, a lock of up to 12 hours and up to 10,000 partitions.";
            return null;
        }
        if (!IsNew && CurrentPartitions is { } current && Partitions < current)
        {
            Error = $"The {KindName} has {current.ToString(CultureInfo.CurrentCulture)} partitions; Kafka cannot remove partitions.";
            return null;
        }

        TimeSpan? timeToLive = null;
        if (ShowTimeToLive && TimeToLive is { } ttl)
        {
            try
            {
                timeToLive = TimeToLiveUnit switch { TimeUnit.Minutes => TimeSpan.FromMinutes(ttl), TimeUnit.Hours => TimeSpan.FromHours(ttl), _ => TimeSpan.FromDays(ttl) };
            }
            catch (OverflowException)
            {
                Error = "The time to keep unread messages is too long.";
                return null;
            }
        }

        return new QueueSettings(
            timeToLive,
            ShowMaxDeliveryCount ? MaxDeliveryCount : null,
            ShowLockDuration && LockSeconds is { } seconds ? TimeSpan.FromSeconds(seconds) : null,
            ShowDeadLetterOnExpiration ? DeadLetterOnExpiration : null,
            ShowPartitions ? Partitions : null);
    }

    public QueueDefinition? TryBuildDefinition() =>
        TryBuildSettings() is { } settings
            ? new QueueDefinition(Name.Trim(), settings, ShowCreateDeadLetterQueue && CreateDeadLetterQueue, TopicName)
            : null;

    private bool Shows(QueueSettingFlags flag) => ((IsNew ? _capabilities.OnCreate : _capabilities.OnUpdate) & flag) != 0;
}
