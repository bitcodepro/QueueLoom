using System.Collections.ObjectModel;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using QueueLoom.App.Commands;
using QueueLoom.App.Models;
using QueueLoom.App.Services;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Diagnostics;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

public sealed partial class MainWindowViewModel : ObservableObject, IAsyncDisposable
{
    private const string AllEnvironmentsMonitorScope = "All environments";
    private const string CurrentEnvironmentMonitorScope = "Current environment";
    private const string SelectedSourceMonitorScope = "Selected queue / subscription";
    private const string ExplorerMonitorTarget = "Explorer selection";
    private const string DeadLettersMonitorTarget = "Dead letters selection";

    private readonly IProfileRepository _profileRepository;
    private readonly ISecretVault _secretVault;
    private readonly IServiceBusWorkspace _workspace;
    private readonly IUserDialogService _dialogs;
    private readonly IDeadLetterBackupRepository? _backupRepository;
    private readonly IClipboardService? _clipboard;
    private readonly IAppLauncher? _launcher;
    private readonly INotificationService? _notifications;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _workspaceGate = new(1, 1);
    private readonly Dictionary<string, long> _previousDlqCounts = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _monitorBaseline = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _lastDlqMeasurements = new(StringComparer.Ordinal);
    private readonly Dictionary<string, MonitorNotificationItemViewModel> _monitorNotifications = new(StringComparer.Ordinal);
    private readonly List<EntityItemViewModel> _allEntities = [];

    private NavigationItem _selectedNavigation;
    private ProfileItemViewModel? _selectedProfile;
    private ServiceBusProfile? _connectedProfile;
    private EntityItemViewModel? _selectedEntity;
    private DeadLetterEnvironmentFilterItemViewModel? _selectedDeadLetterEnvironmentFilter;
    private DlqSourceItemViewModel? _selectedDlqSource;
    private Guid? _preferredDlqSourceProfileId;
    private ServiceBusEntityReference? _preferredDlqSourceEntity;
    private ServiceBusSubQueue? _preferredDlqSourceSubQueue;
    private MessageItemViewModel? _selectedMessage;
    private BackupMessageItemViewModel? _selectedBackup;
    private BackupGroupItemViewModel? _selectedBackupGroup;
    private MessageItemViewModel? _selectedBackupMessage;
    private string _backupFilterText = string.Empty;
    private string _backupStatus = "Open the Backups page to inspect local purge backups.";
    private DestinationItemViewModel? _selectedDestination;
    private BrowsedMessage? _draftSourceMessage;
    private bool _draftSourceIsLocalBackup;
    private bool _isBusy;
    private string _statusText = "Ready";
    private string _errorText = string.Empty;
    private string _searchText = string.Empty;
    private string _deadLetterSearchQuery = string.Empty;
    private string _deadLetterSearchStatus = "Search Correlation ID, Message ID, body, or application properties.";
    private string _messageListTitle = "Peeked messages";
    private DateTimeOffset? _lastUpdated;
    private CancellationTokenSource? _monitorCancellation;
    private CancellationTokenSource? _monitorCheckCancellation;
    private CancellationTokenSource? _writeUnlockCancellation;
    private CancellationTokenSource? _currentOperationCancellation;
    private Task? _monitorTask;
    private Task? _writeUnlockTask;
    private Guid? _writeUnlockProfileId;
    private DateTimeOffset? _writeUnlockExpiresAt;
    private bool _isMonitoring;
    private string _monitorScope = CurrentEnvironmentMonitorScope;
    private string _monitorTargetChoice = ExplorerMonitorTarget;
    private int _monitorIntervalSeconds = 60;
    private string _monitorStatus = "Monitor is stopped";
    private string _monitorAlert = string.Empty;
    private bool _hasMonitorBaseline;
    private Guid? _monitoredProfileId;
    private ServiceBusEntityReference? _monitoredEntity;
    private string? _activeMonitorScope;
    private string? _activeMonitorTargetLabel;
    private int _activeMonitorIntervalSeconds;
    private string _draftBody = "{\n  \"event\": \"example\"\n}";
    private MessageBodyFormat _draftBodyFormat = MessageBodyFormat.Json;
    private string _draftMessageId = Guid.NewGuid().ToString("N");
    private string _draftCorrelationId = string.Empty;
    private string _draftSubject = string.Empty;
    private string _draftContentType = "application/json";
    private string _draftSessionId = string.Empty;
    private string _draftTo = string.Empty;
    private string _draftReplyTo = string.Empty;
    private string _draftReplyToSessionId = string.Empty;
    private string _draftPartitionKey = string.Empty;
    private string _draftTransactionPartitionKey = string.Empty;
    private string _draftScheduledEnqueueTime = string.Empty;
    private string _draftTimeToLiveSeconds = string.Empty;
    private string _draftApplicationProperties = "{}";
    private string _draftOriginNotice = "New message";
    private bool _draftMovesOriginal;
    private Guid? _draftProfileId;
    private string? _draftProfileName;
    private bool _lastDlqScanHadFailures;
    private bool _backupsLoaded;
    private bool _pendingBackupRefresh;
    private ServiceBusTopology? _topology;
    private bool _isDisposed;

    public MainWindowViewModel(
        IProfileRepository profileRepository,
        ISecretVault secretVault,
        IServiceBusWorkspace workspace,
        IUserDialogService dialogs,
        IDeadLetterBackupRepository? backupRepository = null,
        IActivityJournal? activityJournal = null,
        IBatchReplayStore? replayStore = null,
        IClipboardService? clipboard = null,
        IAppLauncher? launcher = null,
        INotificationService? notifications = null,
        IThemeService? theme = null,
        ILogger<MainWindowViewModel>? logger = null,
        IMonitorAlertService? alerts = null)
    {
        _profileRepository = profileRepository;
        _secretVault = secretVault;
        _workspace = workspace;
        _dialogs = dialogs;
        _backupRepository = backupRepository;
        _activityJournal = activityJournal;
        _replayStore = replayStore;
        _clipboard = clipboard;
        _launcher = launcher;
        _notifications = notifications;
        _theme = theme;
        _logger = logger ?? NullLogger<MainWindowViewModel>.Instance;

        Navigation =
        [
            new NavigationItem(nameof(NavigationPage.Overview), "Overview", "Ctrl+1"),
            new NavigationItem(nameof(NavigationPage.Explorer), "Explorer", "Ctrl+2"),
            new NavigationItem(nameof(NavigationPage.DeadLetters), "Messages / DLQ", "Ctrl+3"),
            new NavigationItem(nameof(NavigationPage.Backups), "Backups", "Ctrl+4"),
            new NavigationItem(nameof(NavigationPage.Composer), "Composer", "Ctrl+5"),
            new NavigationItem(nameof(NavigationPage.Monitors), "Monitors", "Ctrl+6"),
            new NavigationItem(nameof(NavigationPage.Environments), "Environments", "Ctrl+7"),
            new NavigationItem(nameof(NavigationPage.Activity), "Activity", "Ctrl+8")
        ];
        _selectedNavigation = Navigation[0];

        AddEnvironmentCommand = new AsyncRelayCommand(
            token => RunOperationAsync("Saving environment", AddEnvironmentAsync, token, allowCancellation: false),
            () => !IsBusy);
        EditEnvironmentCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Updating environment", EditEnvironmentAsync, token, allowCancellation: false),
            () => !IsBusy && SelectedProfile is not null);
        DeleteEnvironmentCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Deleting environment", DeleteEnvironmentAsync, token, allowCancellation: false),
            () => !IsBusy && SelectedProfile is not null);
        ConnectCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Connecting", ConnectSelectedAsync, token),
            () => !IsBusy && SelectedProfile is not null);
        DisconnectCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Disconnecting", DisconnectSelectedAsync, token),
            () => !IsBusy && IsSelectedProfileConnected);
        RefreshTopologyCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Refreshing topology", RefreshTopologyAsync, token),
            () => !IsBusy && IsConnected);
        ScanCurrentEnvironmentCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Scanning dead letters", ScanCurrentEnvironmentAsync, token),
            () => !IsBusy && IsConnected);
        ScanAllEnvironmentsCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Scanning all environments", ScanAllEnvironmentsAsync, token),
            () => !IsBusy && Profiles.Count > 0);
        SearchDeadLettersCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Searching dead letters", SearchDeadLettersAsync, token),
            () => !IsBusy && Profiles.Count > 0 && !string.IsNullOrWhiteSpace(DeadLetterSearchQuery));
        ClearDeadLetterSearchCommand = new RelayCommand(
            ClearDeadLetterSearch,
            () => !IsBusy && (Messages.Count > 0 || !string.IsNullOrWhiteSpace(DeadLetterSearchQuery)));
        ClearMonitorNotificationsCommand = new RelayCommand(
            ClearMonitorNotifications,
            () => MonitorNotifications.Count > 0);
        CancelCurrentOperationCommand = new RelayCommand(
            CancelCurrentOperation,
            () => IsBusy && _currentOperationCancellation is { IsCancellationRequested: false });
        BrowseSelectedActiveCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Peeking messages", ct => BrowseSelectedEntityAsync(ServiceBusSubQueue.Active, ct), token),
            () => !IsBusy && SelectedEntity?.CanBrowse == true && IsConnected);
        BrowseSelectedDeadLettersCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Peeking DLQ", ct => BrowseSelectedEntityAsync(ServiceBusSubQueue.DeadLetter, ct), token),
            () => !IsBusy && SelectedEntity?.CanBrowse == true && IsConnected);
        BrowseSelectedTransferDeadLettersCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Peeking transfer DLQ", ct => BrowseSelectedEntityAsync(ServiceBusSubQueue.TransferDeadLetter, ct), token),
            () => !IsBusy && SelectedEntity?.CanBrowse == true && IsConnected && SupportsTransferDeadLetter);
        BrowseDlqSourceCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Opening DLQ", BrowseSelectedDlqSourceAsync, token),
            () => !IsBusy && SelectedDlqSource is { Count: > 0 });
        PurgeEnvironmentDeadLettersCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Purging environment dead letters", PurgeEnvironmentDeadLettersAsync, token),
            () => !IsBusy && CanPurgeEnvironmentDeadLetters);
        PurgeTopicDeadLettersCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Purging topic dead letters", PurgeTopicDeadLettersAsync, token),
            () => !IsBusy && CanPurgeTopicDeadLetters);
        PurgeSelectedDeadLettersCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Purging selected dead letters", PurgeSelectedDeadLettersAsync, token),
            () => !IsBusy && CanPurgeSelectedDeadLetters);
        NewMessageCommand = new RelayCommand(NewMessage, () => !IsBusy && IsConnected);
        OpenMessageAsDraftCommand = new RelayCommand(
            OpenSelectedMessageAsDraft,
            () => !IsBusy && CanOpenSelectedMessageAsDraft);
        RefreshBackupsCommand = new AsyncRelayCommand(
            token => RunOperationAsync("Loading backups", RefreshBackupsAsync, token),
            () => !IsBusy && _backupRepository is not null);
        LoadSelectedBackupCommand = new AsyncRelayCommand(
            token => RunOperationAsync("Loading backup message", LoadSelectedBackupAsync, token),
            () => !IsBusy && SelectedBackup?.IsReadable == true && _backupRepository is not null);
        DeleteSelectedBackupCommand = new AsyncRelayCommand(
            token => RunOperationAsync("Deleting local backup", DeleteSelectedBackupAsync, token, allowCancellation: false),
            () => !IsBusy && SelectedBackup is not null && _backupRepository is not null);
        DeleteVisibleBackupsCommand = new AsyncRelayCommand(
            token => RunOperationAsync("Deleting local backups", DeleteVisibleBackupsAsync, token, allowCancellation: false),
            () => !IsBusy && FilteredBackupMessages.Count > 0 && _backupRepository is not null);
        OpenBackupAsDraftCommand = new RelayCommand(
            OpenBackupAsDraft,
            () => !IsBusy && CanOpenBackupAsDraft);
        SendDraftCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Sending message", SendDraftAsync, token, allowCancellation: false),
            () => !IsBusy && IsConnected && CanWrite &&
                  !HasDraftEnvironmentMismatch && SelectedDestination is not null);
        ToggleMonitorCommand = new AsyncRelayCommand(
            token => RunGuardedAsync("Monitor", ToggleMonitorAsync, token),
            () => IsMonitoring || (!IsBusy && Profiles.Count > 0));
        UnlockWritesCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Unlocking writes", UnlockWritesAsync, token, allowCancellation: false),
            () => !IsBusy && IsConnected && !CanWrite);

        InitializeOperationsFeatures();
        InitializePreferences();
        InitializePresentation();
        InitializeMessageDeletion();
        InitializeResend();
        InitializeExport();
        InitializeSavedSearches();
        InitializeAlerts(alerts);
        RefreshDeadLetterEnvironmentFilters();
    }

    public IReadOnlyList<NavigationItem> Navigation { get; }

    public ObservableCollection<ProfileItemViewModel> Profiles { get; } = [];

    public ObservableCollection<EntityItemViewModel> Entities { get; } = [];

    public ObservableCollection<DlqSourceItemViewModel> DeadLetterSources { get; } = [];

    public ObservableCollection<DlqSourceItemViewModel> FilteredDeadLetterSources { get; } = [];

    public ObservableCollection<DeadLetterEnvironmentFilterItemViewModel> DeadLetterEnvironmentFilters { get; } = [];

    public ObservableCollection<MessageItemViewModel> Messages { get; } = [];

    public ObservableCollection<BackupMessageItemViewModel> BackupMessages { get; } = [];

    public ObservableCollection<BackupMessageItemViewModel> FilteredBackupMessages { get; } = [];

    /// <summary>All backups, environments, topics with their subscriptions, and queues, with counts.</summary>
    public ObservableCollection<BackupGroupItemViewModel> BackupGroups { get; } = [];

    public ObservableCollection<DestinationItemViewModel> Destinations { get; } = [];

    public ObservableCollection<ActivityItemViewModel> Activity { get; } = [];

    public ObservableCollection<MonitorNotificationItemViewModel> MonitorNotifications { get; } = [];

    public IReadOnlyList<MessageBodyFormat> MessageBodyFormats { get; } = Enum.GetValues<MessageBodyFormat>();

    public IReadOnlyList<string> MonitorScopes { get; } =
        [CurrentEnvironmentMonitorScope, SelectedSourceMonitorScope];

    public IReadOnlyList<string> MonitorTargetChoices { get; } =
        [ExplorerMonitorTarget, DeadLettersMonitorTarget];

    public AsyncRelayCommand AddEnvironmentCommand { get; }
    public AsyncRelayCommand EditEnvironmentCommand { get; }
    public AsyncRelayCommand DeleteEnvironmentCommand { get; }
    public AsyncRelayCommand ConnectCommand { get; }
    public AsyncRelayCommand DisconnectCommand { get; }
    public AsyncRelayCommand RefreshTopologyCommand { get; }
    public AsyncRelayCommand ScanCurrentEnvironmentCommand { get; }
    public AsyncRelayCommand ScanAllEnvironmentsCommand { get; }
    public AsyncRelayCommand SearchDeadLettersCommand { get; }
    public RelayCommand ClearDeadLetterSearchCommand { get; }
    public RelayCommand ClearMonitorNotificationsCommand { get; }
    public RelayCommand CancelCurrentOperationCommand { get; }
    public AsyncRelayCommand BrowseSelectedActiveCommand { get; }
    public AsyncRelayCommand BrowseSelectedDeadLettersCommand { get; }
    public AsyncRelayCommand BrowseSelectedTransferDeadLettersCommand { get; }
    public AsyncRelayCommand BrowseDlqSourceCommand { get; }
    public AsyncRelayCommand PurgeEnvironmentDeadLettersCommand { get; }
    public AsyncRelayCommand PurgeTopicDeadLettersCommand { get; }
    public AsyncRelayCommand PurgeSelectedDeadLettersCommand { get; }
    public RelayCommand NewMessageCommand { get; }
    public RelayCommand OpenMessageAsDraftCommand { get; }
    public AsyncRelayCommand RefreshBackupsCommand { get; }
    public AsyncRelayCommand LoadSelectedBackupCommand { get; }
    public AsyncRelayCommand DeleteSelectedBackupCommand { get; }
    public AsyncRelayCommand DeleteVisibleBackupsCommand { get; }
    public RelayCommand OpenBackupAsDraftCommand { get; }
    public AsyncRelayCommand SendDraftCommand { get; }
    public AsyncRelayCommand ToggleMonitorCommand { get; }
    public AsyncRelayCommand UnlockWritesCommand { get; }

    public NavigationItem SelectedNavigation
    {
        get => _selectedNavigation;
        set
        {
            if (value is null || !SetProperty(ref _selectedNavigation, value))
            {
                return;
            }

            OnPropertyChanged(nameof(CurrentPage));
            if (CurrentPage == NavigationPage.Backups && _backupRepository is not null)
            {
                if (IsBusy)
                {
                    _pendingBackupRefresh = true;
                }
                else if (!_backupsLoaded)
                {
                    RefreshBackupsCommand.Execute(null);
                }
            }
        }
    }

    public NavigationPage CurrentPage => Enum.TryParse<NavigationPage>(SelectedNavigation.Key, out var page)
        ? page
        : NavigationPage.Overview;

    public bool IsBusy
    {
        get => _isBusy;
        private set
        {
            if (SetProperty(ref _isBusy, value))
            {
                NotifyCommandStates();
            }
        }
    }

    public string StatusText
    {
        get => _statusText;
        private set => SetProperty(ref _statusText, value);
    }

    public string ErrorText
    {
        get => _errorText;
        private set
        {
            if (SetProperty(ref _errorText, value))
            {
                OnPropertyChanged(nameof(HasError));
            }
        }
    }

    public bool HasError => !string.IsNullOrWhiteSpace(ErrorText);

    public string CancelOperationLabel =>
        _currentOperationCancellation?.IsCancellationRequested == true ? "Cancelling…" : "Cancel";

    public bool ShowCancelCurrentOperation =>
        IsBusy && _currentOperationCancellation is { IsCancellationRequested: false };

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await RunOperationAsync("Loading environments", async token =>
        {
            LoadActivityHistory();
            await ReloadProfilesAsync(token).ConfigureAwait(true);
            StatusText = Profiles.Count == 0
                ? "Add your first environment to begin"
                : "Choose an environment and connect";
        }, cancellationToken).ConfigureAwait(true);
    }

    private async Task RunWorkspaceOperationAsync(
        string operation,
        Func<CancellationToken, Task> action,
        CancellationToken cancellationToken,
        bool allowCancellation = true)
    {
        // Interactive work wins over a background monitor traversal. Cancelling the
        // per-check token leaves the monitor itself running for its next interval.
        _monitorCheckCancellation?.Cancel();
        await RunOperationAsync(
                operation,
                async token =>
                {
                    await _workspaceGate.WaitAsync(token).ConfigureAwait(true);
                    try
                    {
                        await action(token).ConfigureAwait(true);
                    }
                    finally
                    {
                        _workspaceGate.Release();
                    }
                },
                cancellationToken,
                allowCancellation)
            .ConfigureAwait(true);
    }

    private async Task RunOperationAsync(
        string operation,
        Func<CancellationToken, Task> action,
        CancellationToken cancellationToken,
        bool allowCancellation = true)
    {
        using var operationCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _operationId = Guid.NewGuid();
        _currentOperationCancellation = allowCancellation ? operationCancellation : null;
        IsBusy = true;
        NotifyCurrentOperationCancellationState();
        ErrorText = string.Empty;
        StatusText = operation;
        try
        {
            await action(operationCancellation.Token).ConfigureAwait(true);
            if (StatusText == operation)
            {
                // The action reported nothing more specific; do not leave a stale "in progress" text.
                StatusText = $"{operation} completed";
            }
        }
        catch (OperationCanceledException) when (operationCancellation.IsCancellationRequested)
        {
            StatusText = "Operation cancelled";
            AddActivity("Warning", operation, "Cancelled; completed changes are retained. Inspect the saved operation result.");
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "{Operation} failed", operation);
            ErrorText = SanitizeException(exception);
            StatusText = $"{operation} failed";
            AddActivity("Error", operation, ErrorText);
        }
        finally
        {
            if (ReferenceEquals(_currentOperationCancellation, operationCancellation))
            {
                _currentOperationCancellation = null;
            }
            IsBusy = false;
            NotifyCurrentOperationCancellationState();
        }

        if (TryConsumePendingBackupRefresh())
        {
            await RunOperationAsync(
                    "Loading backups",
                    RefreshBackupsAsync,
                    CancellationToken.None)
                .ConfigureAwait(true);
        }
    }

    /// <summary>
    /// Runs work that must not take the busy state (the monitor toggle stays available while
    /// other operations run) but whose failures must still reach the operator, not crash the app.
    /// </summary>
    private async Task RunGuardedAsync(string operation, Func<CancellationToken, Task> action, CancellationToken cancellationToken)
    {
        try
        {
            await action(cancellationToken).ConfigureAwait(true);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "{Operation} failed", operation);
            ErrorText = SanitizeException(exception);
            AddActivity("Error", operation, ErrorText);
        }
    }

    private void CancelCurrentOperation()
    {
        if (_currentOperationCancellation is null)
        {
            return;
        }

        StatusText = "Cancelling…";
        _currentOperationCancellation.Cancel();
        NotifyCurrentOperationCancellationState();
    }

    private void NotifyCurrentOperationCancellationState()
    {
        OnPropertyChanged(nameof(CancelOperationLabel));
        OnPropertyChanged(nameof(ShowCancelCurrentOperation));
        CancelCurrentOperationCommand.NotifyCanExecuteChanged();
    }

    private bool TryConsumePendingBackupRefresh()
    {
        if (!_pendingBackupRefresh ||
            _isDisposed ||
            _backupRepository is null ||
            IsBusy ||
            CurrentPage != NavigationPage.Backups)
        {
            if (CurrentPage != NavigationPage.Backups)
            {
                _pendingBackupRefresh = false;
            }
            return false;
        }

        _pendingBackupRefresh = false;
        return true;
    }

    private void NavigateTo(NavigationPage page)
    {
        SelectedNavigation = Navigation.First(item => item.Key == page.ToString());
    }

    private void NotifyCommandStates()
    {
        EditEnvironmentCommand.NotifyCanExecuteChanged();
        DeleteEnvironmentCommand.NotifyCanExecuteChanged();
        ConnectCommand.NotifyCanExecuteChanged();
        DisconnectCommand.NotifyCanExecuteChanged();
        RefreshTopologyCommand.NotifyCanExecuteChanged();
        ScanCurrentEnvironmentCommand.NotifyCanExecuteChanged();
        ScanAllEnvironmentsCommand.NotifyCanExecuteChanged();
        SearchDeadLettersCommand.NotifyCanExecuteChanged();
        ClearDeadLetterSearchCommand.NotifyCanExecuteChanged();
        CancelCurrentOperationCommand.NotifyCanExecuteChanged();
        BrowseSelectedActiveCommand.NotifyCanExecuteChanged();
        BrowseSelectedDeadLettersCommand.NotifyCanExecuteChanged();
        BrowseSelectedTransferDeadLettersCommand.NotifyCanExecuteChanged();
        BrowseDlqSourceCommand.NotifyCanExecuteChanged();
        PurgeEnvironmentDeadLettersCommand.NotifyCanExecuteChanged();
        PurgeTopicDeadLettersCommand.NotifyCanExecuteChanged();
        PurgeSelectedDeadLettersCommand.NotifyCanExecuteChanged();
        NewMessageCommand.NotifyCanExecuteChanged();
        OpenMessageAsDraftCommand.NotifyCanExecuteChanged();
        RefreshBackupsCommand.NotifyCanExecuteChanged();
        LoadSelectedBackupCommand.NotifyCanExecuteChanged();
        DeleteSelectedBackupCommand.NotifyCanExecuteChanged();
        DeleteVisibleBackupsCommand.NotifyCanExecuteChanged();
        OpenBackupAsDraftCommand.NotifyCanExecuteChanged();
        SendDraftCommand.NotifyCanExecuteChanged();
        ToggleMonitorCommand.NotifyCanExecuteChanged();
        UnlockWritesCommand.NotifyCanExecuteChanged();
        DeleteMarkedMessagesCommand?.NotifyCanExecuteChanged();
    }

    private void AddActivity(
        string level,
        string action,
        string details,
        ServiceBusEntityReference? source = null)
    {
        LogActivity(level, action, details, source);
        PersistActivity(level, action, details, source);
        Activity.Insert(0, new ActivityItemViewModel(DateTimeOffset.UtcNow, level, action, details, source));
        while (Activity.Count > 500)
        {
            Activity.RemoveAt(Activity.Count - 1);
        }
    }

    private static string SanitizeException(Exception exception) =>
        SensitiveDataRedactor.SummarizeException(exception);

    private static string FormatSubQueue(ServiceBusSubQueue subQueue) => subQueue switch
    {
        ServiceBusSubQueue.Active => "Active messages",
        ServiceBusSubQueue.DeadLetter => "Dead-letter queue",
        ServiceBusSubQueue.TransferDeadLetter => "Transfer dead-letter queue",
        _ => subQueue.ToString()
    };

    private static string? NullIfWhiteSpace(string? value) =>
        string.IsNullOrWhiteSpace(value) ? null : value;

    public async ValueTask DisposeAsync()
    {
        if (_isDisposed)
        {
            return;
        }
        _isDisposed = true;

        _monitorCancellation?.Cancel();
        _writeUnlockCancellation?.Cancel();
        _currentOperationCancellation?.Cancel();

        var commands = new[]
        {
            AddEnvironmentCommand,
            EditEnvironmentCommand,
            DeleteEnvironmentCommand,
            ConnectCommand,
            DisconnectCommand,
            RefreshTopologyCommand,
            ScanCurrentEnvironmentCommand,
            ScanAllEnvironmentsCommand,
            SearchDeadLettersCommand,
            BrowseSelectedActiveCommand,
            BrowseSelectedDeadLettersCommand,
            BrowseSelectedTransferDeadLettersCommand,
            BrowseDlqSourceCommand,
            RefreshBackupsCommand,
            LoadSelectedBackupCommand,
            DeleteSelectedBackupCommand,
            DeleteVisibleBackupsCommand,
            PurgeEnvironmentDeadLettersCommand,
            PurgeTopicDeadLettersCommand,
            PurgeSelectedDeadLettersCommand,
            DeleteMarkedMessagesCommand,
            ResendMarkedMessagesCommand,
            ExportMessagesCommand,
            SendTestAlertCommand,
            SendDraftCommand,
            ToggleMonitorCommand,
            UnlockWritesCommand
        };
        foreach (var command in commands)
        {
            command.Cancel();
        }

        var pending = commands.Select(command => command.Completion).ToList();
        if (_monitorTask is not null)
        {
            pending.Add(_monitorTask);
        }
        if (_writeUnlockTask is not null)
        {
            pending.Add(_writeUnlockTask);
        }
        try
        {
            await Task.WhenAll(pending).ConfigureAwait(true);
        }
        catch (Exception exception)
        {
            // Shutdown continues after all tasks have reached a terminal state.
            _logger.LogWarning(exception, "A background operation failed during shutdown");
        }

        _monitorCancellation?.Dispose();
        _monitorCancellation = null;
        _monitorTask = null;
        _writeUnlockCancellation?.Dispose();
        _writeUnlockCancellation = null;
        _writeUnlockTask = null;

        foreach (var command in commands)
        {
            command.Dispose();
        }

        await _workspace.DisposeAsync().ConfigureAwait(true);
        _workspaceGate.Dispose();
    }
}
