using System.Text.Json;
using QueueLoom.App.Commands;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.ServiceBus;
using QueueLoom.App.Serialization;
using QueueLoom.Core.Validation;

namespace QueueLoom.App.ViewModels;

public sealed partial class MainWindowViewModel
{
    private readonly IActivityJournal? _activityJournal;
    private Guid _operationId = Guid.NewGuid();
    private bool _activityLoaded;
    private ProfileItemViewModel? _browseProfile;
    private ServiceBusEntityReference? _browseSource;
    private ServiceBusSubQueue _browseSubQueue;
    private long? _browseCursor;
    private bool _browseExhausted;
    private bool _browseDisplayLimit;
    private decimal? _purgeLimitPerSource = 1000;
    private bool _hasDlqScan;
    public string GlobalDlqDisplay => _hasDlqScan ? GlobalDlqSourceCount.ToString("N0") : "—";
    public decimal? PurgeLimitPerSource
    {
        get => _purgeLimitPerSource;
        set
        {
            if (SetProperty(ref _purgeLimitPerSource, value))
            {
                OnPropertyChanged(nameof(HasValidPurgeLimit));
                OnPropertyChanged(nameof(PurgeLimitHint));
            }
        }
    }
    public bool HasValidPurgeLimit => PurgeLimitPerSource is >= 1 and <= 10000 &&
        decimal.Truncate(PurgeLimitPerSource.Value) == PurgeLimitPerSource.Value;
    public string PurgeLimitHint => HasValidPurgeLimit
        ? "Maximum messages per queue or subscription. Backup is saved before deletion."
        : "Enter a whole number from 1 to 10,000 to continue.";
    public AsyncRelayCommand LoadMoreMessagesCommand { get; private set; } = null!;
    public RelayCommand FormatJsonCommand { get; private set; } = null!;
    public RelayCommand GenerateMessageIdCommand { get; private set; } = null!;
    public RelayCommand AddApplicationPropertyCommand { get; private set; } = null!;
    public string PropertyName { get; set; } = string.Empty;
    public string PropertyValue { get; set; } = string.Empty;
    public ApplicationPropertyType PropertyType { get; set; } = ApplicationPropertyType.String;
    public IReadOnlyList<ApplicationPropertyType> PropertyTypes { get; } = Enum.GetValues<ApplicationPropertyType>();
    public bool CanLoadMoreMessages => !IsBusy && !_browseExhausted && _browseSource is not null &&
        _browseProfile?.Id == ConnectedProfileId && Messages.Count < 1000 && RetainedBrowseBytes < 32 * 1024 * 1024;
    private long RetainedBrowseBytes => Messages.Sum(m => (long)m.Message.Body.Length);
    public bool HasMessages => Messages.Count > 0;
    public bool HasEntities => Entities.Count > 0;
    public bool UsesSampledCounts => _topology?.UsesSampledCounts == true;
    public string EmptyMessagesText => !IsConnected ? "Connect an environment to browse messages." : "Select a source and Peek, or search dead letters.";
    public string BrowsePageStatus => _browseSource is null ? DeadLetterSearchStatus :
        $"{Messages.Count:N0} loaded · {RetainedBrowseBytes / 1024d:N1} KiB retained · " +
        (_browseDisplayLimit || Messages.Count >= 1000 ? "Display limit reached (1,000 messages / 32 MiB)" :
            _browseExhausted ? "End of available messages" : "Use Load next 100 to continue");

    private void InitializeOperationsFeatures()
    {
        LoadMoreMessagesCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Loading next page", LoadBrowsePageAsync, token), () => CanLoadMoreMessages);
        FormatJsonCommand = new RelayCommand(() =>
        {
            try
            {
                using var document = JsonDocument.Parse(DraftBody);
                DraftBody = JsonSerializer.Serialize(document.RootElement, new JsonSerializerOptions { WriteIndented = true });
                ErrorText = string.Empty;
            }
            catch (JsonException exception) { ErrorText = $"JSON: line {exception.LineNumber + 1}, column {exception.BytePositionInLine + 1}: {exception.Message}"; }
        }, () => !IsBusy);
        GenerateMessageIdCommand = new RelayCommand(() => DraftMessageId = Guid.NewGuid().ToString("N"), () => !IsBusy);
        AddApplicationPropertyCommand = new RelayCommand(() =>
        {
            try
            {
                var properties = ApplicationPropertiesJson.Deserialize(DraftApplicationProperties).ToList();
                var property = new MessageApplicationProperty(PropertyName.Trim(), PropertyType, PropertyValue);
                properties.RemoveAll(p => p.Name == property.Name);
                properties.Add(property);
                var validation = MessageDraftValidator.Validate(new MessageDraft(EditableMessageBody.Empty, applicationProperties: properties));
                if (!validation.IsValid) throw new InvalidOperationException(string.Join(" ", validation.Errors.Select(e => e.Message)));
                DraftApplicationProperties = ApplicationPropertiesJson.Serialize(properties);
                ErrorText = string.Empty;
            }
            catch (Exception exception) { ErrorText = SanitizeException(exception); }
        }, () => !IsBusy);
        InitializeReplayFeatures();
        Messages.CollectionChanged += (_, _) => NotifyBrowseFeatures();
        Entities.CollectionChanged += (_, _) => OnPropertyChanged(nameof(HasEntities));
        PropertyChanged += (_, args) =>
        {
            if (args.PropertyName is nameof(IsBusy) or nameof(IsConnected) or nameof(ConnectedProfileId) or nameof(CanWrite))
                NotifyBrowseFeatures();
        };
    }

    private void ResetBrowsePaging()
    {
        _browseProfile = null; _browseSource = null; _browseCursor = null; _browseExhausted = false; _browseDisplayLimit = false;
        NotifyBrowseFeatures();
    }

    private async Task LoadBrowsePageAsync(CancellationToken token)
    {
        var profile = _browseProfile ?? throw new InvalidOperationException("Select a source again.");
        var source = _browseSource ?? throw new InvalidOperationException("Select a source again.");
        if (ConnectedProfileId != profile.Id) throw new InvalidOperationException("Reconnect the source environment first.");
        var page = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(source, _browseSubQueue,
            maxMessages: 100, fromSequenceNumber: _browseCursor), token).ConfigureAwait(true);
        var seen = Messages.Select(m => m.Message.SequenceNumber).ToHashSet();
        var bytes = RetainedBrowseBytes;
        foreach (var message in page.OrderBy(m => m.SequenceNumber))
        {
            if (Messages.Count >= 1000 || bytes + message.Body.Length > 32 * 1024 * 1024)
            { _browseExhausted = true; _browseDisplayLimit = true; break; }
            if (!seen.Add(message.SequenceNumber)) continue;
            Messages.Add(new MessageItemViewModel(message, profile.Id, profile.Name, profile.EnvironmentLabel, profile.EnvironmentColor));
            bytes += message.Body.Length;
            _browseCursor = message.SequenceNumber == long.MaxValue ? null : message.SequenceNumber + 1;
            if (message.SequenceNumber == long.MaxValue) _browseExhausted = true;
        }
        _browseExhausted |= page.Count < 100;
        SelectedMessage ??= Messages.FirstOrDefault();
        NotifyBrowseFeatures();
    }

    private void NotifyBrowseFeatures()
    {
        OnPropertyChanged(nameof(HasMessages)); OnPropertyChanged(nameof(EmptyMessagesText));
        OnPropertyChanged(nameof(CanLoadMoreMessages)); OnPropertyChanged(nameof(BrowsePageStatus));
        LoadMoreMessagesCommand?.NotifyCanExecuteChanged();
        FormatJsonCommand?.NotifyCanExecuteChanged(); GenerateMessageIdCommand?.NotifyCanExecuteChanged();
        AddApplicationPropertyCommand?.NotifyCanExecuteChanged();
        NotifyReplayFeatures();
    }

    private ActivityRecord MakeActivity(string level, string action, string details, ServiceBusEntityReference? source) =>
        new(_operationId, DateTimeOffset.UtcNow, level, action, SensitiveValuePattern.Replace(details, "$1=[REDACTED]"),
            ConnectedProfileId, _connectedProfile?.Name, source);

    private void RecordOperationIntent(string action, string details, ServiceBusEntityReference? source)
    {
        _operationId = Guid.NewGuid();
        // Fail closed if the durable record cannot be written before a destructive action.
        _activityJournal?.Append(MakeActivity("Warning", action, details, source));
    }

    private void PersistActivity(string level, string action, string details, ServiceBusEntityReference? source)
    {
        try { _activityJournal?.Append(MakeActivity(level, action, details, source)); }
        catch (Exception exception) { ErrorText = $"Activity journal could not be saved: {SanitizeException(exception)}"; }
    }

    private void LoadActivityHistory()
    {
        if (_activityLoaded || _activityJournal is null) return;
        foreach (var item in _activityJournal.ReadRecent())
            Activity.Add(new ActivityItemViewModel(item.Timestamp, item.Level, item.Action,
                $"{item.ProfileName} · {item.Details} · operation {item.OperationId:N}", item.Source));
        _activityLoaded = true;
    }
}
