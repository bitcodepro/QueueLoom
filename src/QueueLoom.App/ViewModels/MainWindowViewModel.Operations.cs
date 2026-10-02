using System.Globalization;
using System.Text.Json;
using QueueLoom.App.Commands;
using Microsoft.Extensions.Logging;
using QueueLoom.App.Services;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Diagnostics;
using QueueLoom.Core.Profiles;
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
    public string GlobalDlqDisplay => _hasDlqScan ? GlobalDlqSourceCount.ToString("N0", CultureInfo.CurrentCulture) : "—";
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
    public RelayCommand<string> CopyTextCommand { get; private set; } = null!;
    public AsyncRelayCommand OpenBackupsFolderCommand { get; private set; } = null!;
    public string PropertyName { get; set; } = string.Empty;
    public string PropertyValue { get; set; } = string.Empty;
    public ApplicationPropertyType PropertyType { get; set; } = ApplicationPropertyType.String;
    public IReadOnlyList<ApplicationPropertyType> PropertyTypes { get; } = Enum.GetValues<ApplicationPropertyType>();
    public bool CanLoadMoreMessages => !IsBusy && !_browseExhausted && _browseSource is not null &&
        _browseProfile?.Id == ConnectedProfileId && Messages.Count < BrowseDisplayLimit && RetainedBrowseBytes < BrowseByteLimit;
    private long RetainedBrowseBytes => Messages.Sum(m => (long)m.Message.Body.Length);
    private const int BrowseDisplayLimit = 10_000;
    private const long BrowseByteLimit = 128L * 1024 * 1024;
    public bool HasMessages => Messages.Count > 0;
    public bool HasEntities => Entities.Count > 0;
    public bool UsesSampledCounts => _topology?.UsesSampledCounts == true;

    /// <summary>Only Azure Service Bus (and not its emulator) has transfer dead-letter queues.</summary>
    public bool SupportsTransferDeadLetter => _topology is { SupportsTransferDeadLetter: true, UsesSampledCounts: false };
    public string EmptyMessagesText => !IsConnected ? "Connect an environment to browse messages." : "Select a source and Peek, or search dead letters.";
    public string BrowsePageStatus => _browseSource is null ? DeadLetterSearchStatus :
        $"{Messages.Count:N0} loaded · {RetainedBrowseBytes / 1024d:N1} KiB retained · " +
        (_browseDisplayLimit || Messages.Count >= BrowseDisplayLimit ? "Display limit reached (10,000 messages / 128 MiB)" :
            _browseExhausted ? "End of available messages" : "Use Load next 100 to continue");

    private void InitializeOperationsFeatures()
    {
        InitializeActivityView();
        LoadMoreMessagesCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Loading next page", LoadBrowsePageAsync, token), () => CanLoadMoreMessages);
        FormatJsonCommand = new RelayCommand(() =>
        {
            try
            {
                using var document = JsonDocument.Parse(DraftBody);
                DraftBody = JsonSerializer.Serialize(document.RootElement, new JsonSerializerOptions { WriteIndented = true });
                DraftBodyErrorLine = null;
                ErrorText = string.Empty;
            }
            catch (JsonException exception)
            {
                var diagnosticOperation = Diagnostics.Begin("Formatting JSON");
                Diagnostics.Record(diagnosticOperation, DiagnosticStage.Failed, error: exception);
                DraftBodyErrorLine = exception.LineNumber is { } line ? (int)line + 1 : null;
                ErrorText = $"JSON: line {exception.LineNumber + 1}, column {exception.BytePositionInLine + 1}: {exception.Message}";
            }
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
        CopyTextCommand = new RelayCommand<string>(text => _ = CopyTextAsync(text), text => !string.IsNullOrEmpty(text));
        OpenBackupsFolderCommand = new AsyncRelayCommand(
            OpenBackupsFolderAsync,
            () => _backupRepository is not null && _launcher is not null);
        InitializeReplayFeatures();
        InitializeLogReading();
        Messages.CollectionChanged += (_, _) =>
        {
            if (_messageBatchDepth > 0) _messagesChangedInBatch = true;
            else NotifyBrowseFeatures();
        };
        Entities.CollectionChanged += (_, _) => OnPropertyChanged(nameof(HasEntities));
        PropertyChanged += (_, args) =>
        {
            if (args.PropertyName is nameof(IsBusy) or nameof(IsConnected) or nameof(ConnectedProfileId) or nameof(CanWrite))
            {
                NotifyBrowseFeatures();
                if (args.PropertyName is not nameof(IsBusy)) UpdateScheduledStatuses();
                RunScheduledResendCommand?.NotifyCanExecuteChanged();
            }
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
        // SQS and Pub/Sub cannot continue from a position: ask for everything shown so far plus 100 more
        // and keep the new ones. Their messages are listed oldest first.
        // Kafka continues from each partition's position, so it pages like a sequence.
        var log = profile.Provider == MessagingProvider.Kafka;
        var positional = log || Messages.Count == 0 || Messages[0].Message.HasSequenceNumber;
        var requested = positional ? 100 : Math.Min(BrowseMessagesRequest.MaximumMaxMessages, Messages.Count + 100);
        var page = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(source, _browseSubQueue,
            maxMessages: requested, fromSequenceNumber: positional && !log ? _browseCursor : null)
        {
            Start = log ? NextLogStart() : BrowseStart.Oldest
        }, token).ConfigureAwait(true);
        if (log)
        {
            AdvanceLogPositions(page);
        }
        using var batch = BatchMessageUpdates();
        var seen = Messages.Select(m => m.Message.SequenceNumber).ToHashSet();
        var bytes = RetainedBrowseBytes;
        var ordered = page.All(m => m.HasSequenceNumber)
            ? page.OrderBy(m => m.SequenceNumber)
            : log && _browseStart.Kind == BrowseStartKind.Newest
                ? page.OrderByDescending(m => m.EnqueuedAt ?? DateTimeOffset.MinValue).ThenByDescending(m => m.Position?.Offset ?? 0)
                : page.OrderBy(m => m.EnqueuedAt ?? DateTimeOffset.MaxValue).ThenBy(m => m.Properties.MessageId, StringComparer.Ordinal);
        foreach (var message in ordered)
        {
            if (Messages.Count >= BrowseDisplayLimit || bytes + message.Body.Length > BrowseByteLimit)
            { _browseExhausted = true; _browseDisplayLimit = true; break; }
            if (!seen.Add(message.SequenceNumber)) continue;
            Messages.Add(new MessageItemViewModel(message, profile.Id, profile.Name, profile.EnvironmentLabel, profile.EnvironmentTone));
            bytes += message.Body.Length;
            _browseCursor = message.SequenceNumber == long.MaxValue ? null : message.SequenceNumber + 1;
            if (message.SequenceNumber == long.MaxValue) _browseExhausted = true;
        }
        _browseExhausted |= page.Count < requested;
        SelectedMessage ??= Messages.FirstOrDefault();
        NotifyBrowseFeatures();
    }

    private void NotifyBrowseFeatures()
    {
        OnPropertyChanged(nameof(HasMessages)); OnPropertyChanged(nameof(EmptyMessagesText));
        OnPropertyChanged(nameof(CanLoadMoreMessages)); OnPropertyChanged(nameof(BrowsePageStatus));
        OnPropertyChanged(nameof(ShowLogReadBar));
        LoadMoreMessagesCommand?.NotifyCanExecuteChanged();
        ReadLogCommand?.NotifyCanExecuteChanged();
        FormatJsonCommand?.NotifyCanExecuteChanged(); GenerateMessageIdCommand?.NotifyCanExecuteChanged();
        AddApplicationPropertyCommand?.NotifyCanExecuteChanged();
        NotifyReplayFeatures();
    }

    private ActivityRecord MakeActivity(string level, string action, string details, ServiceBusEntityReference? source, DateTimeOffset? timestamp = null) =>
        new(_operationId, timestamp ?? DateTimeOffset.UtcNow, level, action, SensitiveDataRedactor.Redact(details),
            ConnectedProfileId, _connectedProfile?.Name, source);

    private void RecordOperationIntent(string action, string details, ServiceBusEntityReference? source)
    {
        _operationId = Guid.NewGuid();
        // Fail closed if the durable record cannot be written before a destructive action.
        _activityJournal?.Append(MakeActivity("Warning", action, details, source));
    }

    private void PersistActivity(string level, string action, string details, ServiceBusEntityReference? source, DateTimeOffset timestamp)
    {
        try { _activityJournal?.Append(MakeActivity(level, action, details, source, timestamp)); }
        catch (Exception exception) { ErrorText = $"Activity journal could not be saved: {SanitizeException(exception)}"; }
    }

    private void LogActivity(string level, string action, string details, ServiceBusEntityReference? source)
    {
        var logLevel = level switch
        {
            "Error" => LogLevel.Error,
            "Warning" => LogLevel.Warning,
            _ => LogLevel.Information
        };
        _logger.Log(logLevel, "{Action}: {Details} ({Source})", action, details, source?.DisplayName ?? "namespace");
    }

    /// <summary>Copies operator-visible text and confirms the result with a short toast.</summary>
    public async Task CopyTextAsync(string? text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return;
        }

        var copied = _clipboard is not null && await _clipboard.SetTextAsync(text).ConfigureAwait(true);
        var preview = text.Length > 80 ? text[..80] + "…" : text;
        StatusText = copied ? $"Copied: {preview}" : "The clipboard is unavailable; nothing was copied.";
        _notifications?.Show(
            copied ? "Copied to clipboard" : "Copy failed",
            copied ? preview : "The clipboard is unavailable; nothing was copied.",
            copied ? NotificationTone.Success : NotificationTone.Warning);
    }

    private async Task OpenBackupsFolderAsync(CancellationToken cancellationToken)
    {
        if (_backupRepository is null || _launcher is null)
        {
            return;
        }

        if (!await _launcher.OpenFolderAsync(_backupRepository.RootDirectory).ConfigureAwait(true))
        {
            _notifications?.Show(
                "Could not open folder",
                $"Open it manually: {_backupRepository.RootDirectory}",
                NotificationTone.Warning);
        }
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
