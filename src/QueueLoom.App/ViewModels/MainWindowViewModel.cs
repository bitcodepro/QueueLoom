using System.Collections.ObjectModel;
using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using QueueLoom.App.Commands;
using QueueLoom.App.Models;
using QueueLoom.App.Serialization;
using QueueLoom.App.Services;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

public sealed class MainWindowViewModel : ObservableObject, IAsyncDisposable
{
    private static readonly Regex SensitiveValuePattern = new(
        @"\b(SharedAccessKey|SharedAccessSignature|sig|password|client_secret)\s*=\s*[^;\s&]+",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.NonBacktracking,
        TimeSpan.FromMilliseconds(100));

    private const string AllEnvironmentsMonitorScope = "All environments";
    private const string CurrentEnvironmentMonitorScope = "Current environment";
    private const string SelectedSourceMonitorScope = "Selected queue / subscription";
    private const string ExplorerMonitorTarget = "Explorer selection";
    private const string DeadLettersMonitorTarget = "Dead letters selection";

    private readonly IProfileRepository _profileRepository;
    private readonly ISecretVault _secretVault;
    private readonly IServiceBusWorkspace _workspace;
    private readonly IUserDialogService _dialogs;
    private readonly SemaphoreSlim _workspaceGate = new(1, 1);
    private readonly Dictionary<string, long> _previousDlqCounts = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _monitorBaseline = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _lastDlqMeasurements = new(StringComparer.Ordinal);
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
    private DestinationItemViewModel? _selectedDestination;
    private BrowsedMessage? _draftSourceMessage;
    private bool _isBusy;
    private string _statusText = "Ready";
    private string _errorText = string.Empty;
    private string _searchText = string.Empty;
    private string _messageListTitle = "Peeked messages";
    private DateTimeOffset? _lastUpdated;
    private CancellationTokenSource? _monitorCancellation;
    private CancellationTokenSource? _writeUnlockCancellation;
    private Task? _monitorTask;
    private Task? _writeUnlockTask;
    private Guid? _writeUnlockProfileId;
    private DateTimeOffset? _writeUnlockExpiresAt;
    private bool _isMonitoring;
    private string _monitorScope = AllEnvironmentsMonitorScope;
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
    private Guid? _draftProfileId;
    private string? _draftProfileName;
    private bool _lastDlqScanHadFailures;
    private ServiceBusTopology? _topology;
    private bool _isDisposed;

    public MainWindowViewModel(
        IProfileRepository profileRepository,
        ISecretVault secretVault,
        IServiceBusWorkspace workspace,
        IUserDialogService dialogs)
    {
        _profileRepository = profileRepository;
        _secretVault = secretVault;
        _workspace = workspace;
        _dialogs = dialogs;

        Navigation =
        [
            new NavigationItem(nameof(NavigationPage.Overview), "01", "Overview"),
            new NavigationItem(nameof(NavigationPage.Explorer), "02", "Explorer"),
            new NavigationItem(nameof(NavigationPage.DeadLetters), "03", "Dead letters"),
            new NavigationItem(nameof(NavigationPage.Composer), "04", "Composer"),
            new NavigationItem(nameof(NavigationPage.Monitors), "05", "Monitors"),
            new NavigationItem(nameof(NavigationPage.Environments), "06", "Environments"),
            new NavigationItem(nameof(NavigationPage.Activity), "07", "Activity")
        ];
        _selectedNavigation = Navigation[0];

        AddEnvironmentCommand = new AsyncRelayCommand(
            token => RunOperationAsync("Saving environment", AddEnvironmentAsync, token),
            () => !IsBusy);
        EditEnvironmentCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Updating environment", EditEnvironmentAsync, token),
            () => !IsBusy && SelectedProfile is not null);
        DeleteEnvironmentCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Deleting environment", DeleteEnvironmentAsync, token),
            () => !IsBusy && SelectedProfile is not null);
        ConnectCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Connecting", ConnectSelectedAsync, token),
            () => !IsBusy && SelectedProfile is not null);
        RefreshTopologyCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Refreshing topology", RefreshTopologyAsync, token),
            () => !IsBusy && IsConnected);
        ScanCurrentEnvironmentCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Scanning dead letters", ScanCurrentEnvironmentAsync, token),
            () => !IsBusy && IsConnected);
        ScanAllEnvironmentsCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Scanning all environments", ScanAllEnvironmentsAsync, token),
            () => !IsBusy && Profiles.Count > 0);
        BrowseSelectedActiveCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Peeking messages", ct => BrowseSelectedEntityAsync(ServiceBusSubQueue.Active, ct), token),
            () => !IsBusy && SelectedEntity?.CanBrowse == true && IsConnected);
        BrowseSelectedDeadLettersCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Peeking DLQ", ct => BrowseSelectedEntityAsync(ServiceBusSubQueue.DeadLetter, ct), token),
            () => !IsBusy && SelectedEntity?.CanBrowse == true && IsConnected);
        BrowseSelectedTransferDeadLettersCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Peeking transfer DLQ", ct => BrowseSelectedEntityAsync(ServiceBusSubQueue.TransferDeadLetter, ct), token),
            () => !IsBusy && SelectedEntity?.CanBrowse == true && IsConnected);
        BrowseDlqSourceCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Opening DLQ", BrowseSelectedDlqSourceAsync, token),
            () => !IsBusy && SelectedDlqSource is { Count: > 0 });
        NewMessageCommand = new RelayCommand(NewMessage, () => !IsBusy && IsConnected);
        OpenMessageAsDraftCommand = new RelayCommand(
            OpenSelectedMessageAsDraft,
            () => !IsBusy && IsConnected && SelectedMessage?.CanOpenAsDraft == true);
        SendDraftCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Sending message", SendDraftAsync, token),
            () => !IsBusy && IsConnected && CanWrite &&
                  !HasDraftEnvironmentMismatch && SelectedDestination is not null);
        ToggleMonitorCommand = new AsyncRelayCommand(
            ToggleMonitorAsync,
            () => IsMonitoring || (!IsBusy && Profiles.Count > 0));
        UnlockWritesCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Unlocking writes", UnlockWritesAsync, token),
            () => !IsBusy && IsConnected && !CanWrite);

        RefreshDeadLetterEnvironmentFilters();
    }

    public IReadOnlyList<NavigationItem> Navigation { get; }

    public ObservableCollection<ProfileItemViewModel> Profiles { get; } = [];

    public ObservableCollection<EntityItemViewModel> Entities { get; } = [];

    public ObservableCollection<DlqSourceItemViewModel> DeadLetterSources { get; } = [];

    public ObservableCollection<DlqSourceItemViewModel> FilteredDeadLetterSources { get; } = [];

    public ObservableCollection<DeadLetterEnvironmentFilterItemViewModel> DeadLetterEnvironmentFilters { get; } = [];

    public ObservableCollection<MessageItemViewModel> Messages { get; } = [];

    public ObservableCollection<DestinationItemViewModel> Destinations { get; } = [];

    public ObservableCollection<ActivityItemViewModel> Activity { get; } = [];

    public IReadOnlyList<MessageBodyFormat> MessageBodyFormats { get; } = Enum.GetValues<MessageBodyFormat>();

    public IReadOnlyList<string> MonitorScopes { get; } =
        [AllEnvironmentsMonitorScope, CurrentEnvironmentMonitorScope, SelectedSourceMonitorScope];

    public IReadOnlyList<string> MonitorTargetChoices { get; } =
        [ExplorerMonitorTarget, DeadLettersMonitorTarget];

    public AsyncRelayCommand AddEnvironmentCommand { get; }
    public AsyncRelayCommand EditEnvironmentCommand { get; }
    public AsyncRelayCommand DeleteEnvironmentCommand { get; }
    public AsyncRelayCommand ConnectCommand { get; }
    public AsyncRelayCommand RefreshTopologyCommand { get; }
    public AsyncRelayCommand ScanCurrentEnvironmentCommand { get; }
    public AsyncRelayCommand ScanAllEnvironmentsCommand { get; }
    public AsyncRelayCommand BrowseSelectedActiveCommand { get; }
    public AsyncRelayCommand BrowseSelectedDeadLettersCommand { get; }
    public AsyncRelayCommand BrowseSelectedTransferDeadLettersCommand { get; }
    public AsyncRelayCommand BrowseDlqSourceCommand { get; }
    public RelayCommand NewMessageCommand { get; }
    public RelayCommand OpenMessageAsDraftCommand { get; }
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
            NotifyPageVisibility();
        }
    }

    public NavigationPage CurrentPage => Enum.TryParse<NavigationPage>(SelectedNavigation.Key, out var page)
        ? page
        : NavigationPage.Overview;

    public bool IsOverviewVisible => CurrentPage == NavigationPage.Overview;
    public bool IsExplorerVisible => CurrentPage == NavigationPage.Explorer;
    public bool IsDeadLettersVisible => CurrentPage == NavigationPage.DeadLetters;
    public bool IsComposerVisible => CurrentPage == NavigationPage.Composer;
    public bool IsMonitorsVisible => CurrentPage == NavigationPage.Monitors;
    public bool IsEnvironmentsVisible => CurrentPage == NavigationPage.Environments;
    public bool IsActivityVisible => CurrentPage == NavigationPage.Activity;

    public ProfileItemViewModel? SelectedProfile
    {
        get => _selectedProfile;
        set
        {
            if (SetProperty(ref _selectedProfile, value))
            {
                OnPropertyChanged(nameof(HasSelectedProfile));
                OnPropertyChanged(nameof(SelectedProfileName));
                OnPropertyChanged(nameof(IsSelectedProfileConnected));
                NotifyCommandStates();
            }
        }
    }

    public bool HasSelectedProfile => SelectedProfile is not null;

    public string SelectedProfileName => SelectedProfile?.Name ?? "No environment selected";

    public bool IsSelectedProfileConnected => SelectedProfile?.IsConnected == true;

    public DeadLetterEnvironmentFilterItemViewModel? SelectedDeadLetterEnvironmentFilter
    {
        get => _selectedDeadLetterEnvironmentFilter;
        set
        {
            if (value is not null && SetProperty(ref _selectedDeadLetterEnvironmentFilter, value))
            {
                ApplyDeadLetterEnvironmentFilter();
            }
        }
    }

    public EntityItemViewModel? SelectedEntity
    {
        get => _selectedEntity;
        set
        {
            if (SetProperty(ref _selectedEntity, value))
            {
                OnPropertyChanged(nameof(HasSelectedEntity));
                OnPropertyChanged(nameof(MonitorTargetPreview));
                NotifyCommandStates();
            }
        }
    }

    public bool HasSelectedEntity => SelectedEntity is not null;

    public DlqSourceItemViewModel? SelectedDlqSource
    {
        get => _selectedDlqSource;
        set
        {
            if (SetProperty(ref _selectedDlqSource, value))
            {
                if (value is not null)
                {
                    _preferredDlqSourceProfileId = value.ProfileId;
                    _preferredDlqSourceEntity = value.Entity;
                    _preferredDlqSourceSubQueue = value.Snapshot.SubQueue;
                }
                OnPropertyChanged(nameof(HasSelectedDlqSource));
                OnPropertyChanged(nameof(MonitorTargetPreview));
                NotifyCommandStates();
            }
        }
    }

    public bool HasSelectedDlqSource => SelectedDlqSource is not null;

    public MessageItemViewModel? SelectedMessage
    {
        get => _selectedMessage;
        set
        {
            if (SetProperty(ref _selectedMessage, value))
            {
                OnPropertyChanged(nameof(HasSelectedMessage));
                NotifyCommandStates();
            }
        }
    }

    public bool HasSelectedMessage => SelectedMessage is not null;

    public DestinationItemViewModel? SelectedDestination
    {
        get => _selectedDestination;
        set
        {
            if (SetProperty(ref _selectedDestination, value))
            {
                NotifyCommandStates();
            }
        }
    }

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

    public bool IsConnected => _workspace.ConnectionState == WorkspaceConnectionState.Connected;

    public Guid? ConnectedProfileId => IsConnected ? _workspace.ConnectedProfileId : null;

    public string ConnectedProfileName => IsConnected
        ? _connectedProfile?.Name ?? "Connected environment"
        : "No environment connected";

    public string ConnectionLabel => IsConnected
        ? $"CONNECTED · {_connectedProfile?.Name ?? "environment"}"
        : "OFFLINE";

    public string ConnectionColor => IsConnected ? "#4ADE9D" : "#91A5BD";

    public string ConnectedNamespace => _connectedProfile?.FullyQualifiedNamespace
        ?? (IsConnected ? "Namespace connection" : "Connect an environment to begin");

    public bool CanWrite => IsConnected &&
                            _connectedProfile?.CanWrite == true &&
                            (_writeUnlockProfileId != _workspace.ConnectedProfileId || IsTemporaryWriteUnlockActive);

    private bool IsTemporaryWriteUnlockActive =>
        _writeUnlockExpiresAt is { } expiresAt && DateTimeOffset.UtcNow < expiresAt;

    public bool CanUnlockWrites => IsConnected && !CanWrite;

    public string WriteAccessLabel => CanWrite ? "WRITE ENABLED" : "READ ONLY";

    public string WriteAccessColor => CanWrite ? "#FFB45E" : "#91A5BD";

    public string SearchText
    {
        get => _searchText;
        set
        {
            if (SetProperty(ref _searchText, value))
            {
                ApplyEntityFilter();
            }
        }
    }

    public string MessageListTitle
    {
        get => _messageListTitle;
        private set => SetProperty(ref _messageListTitle, value);
    }

    public string LastUpdatedText => _lastUpdated.HasValue
        ? $"Updated {_lastUpdated.Value.ToLocalTime():HH:mm:ss}"
        : "Not updated yet";

    public int QueueCount => _topology?.Queues.Count ?? 0;
    public int TopicCount => _topology?.Topics.Count ?? 0;
    public int SubscriptionCount => _topology?.Topics.Sum(topic => topic.Subscriptions.Count) ?? 0;
    public long ActiveMessageCount => _topology?.AggregateMessageCounts.Active ?? 0;
    public long DeadLetterCount => (_topology?.AggregateMessageCounts.DeadLetter ?? 0)
                                   + (_topology?.AggregateMessageCounts.TransferDeadLetter ?? 0);
    public long GlobalDlqSourceCount => DeadLetterSources.Sum(source => source.Count);

    public long VisibleDlqSourceCount => FilteredDeadLetterSources.Sum(source => source.Count);

    public int VisibleDlqSourceRowCount => FilteredDeadLetterSources.Count;

    public bool IsMonitoring
    {
        get => _isMonitoring;
        private set
        {
            if (SetProperty(ref _isMonitoring, value))
            {
                OnPropertyChanged(nameof(MonitorButtonLabel));
                OnPropertyChanged(nameof(MonitorTargetPreview));
            }
        }
    }

    public string MonitorButtonLabel => IsMonitoring ? "Stop monitor" : "Start monitor";

    public string MonitorScope
    {
        get => _monitorScope;
        set
        {
            if (SetProperty(ref _monitorScope, value))
            {
                OnPropertyChanged(nameof(IsSelectedSourceMonitorScope));
                OnPropertyChanged(nameof(MonitorTargetPreview));
            }
        }
    }

    public bool IsSelectedSourceMonitorScope => MonitorScope == SelectedSourceMonitorScope;

    public string MonitorTargetChoice
    {
        get => _monitorTargetChoice;
        set
        {
            if (SetProperty(ref _monitorTargetChoice, value))
            {
                OnPropertyChanged(nameof(MonitorTargetPreview));
            }
        }
    }

    public string MonitorTargetPreview => IsMonitoring && !string.IsNullOrWhiteSpace(_activeMonitorTargetLabel)
        ? $"Pinned: {_activeMonitorTargetLabel}"
        : MonitorTargetChoice == DeadLettersMonitorTarget
            ? SelectedDlqSource is { } source
                ? $"{source.ProfileName} · {source.Entity.DisplayName}"
                : "No source selected in Dead letters"
            : SelectedEntity is { } entity && IsConnected
                ? $"{_connectedProfile?.Name ?? "Connected environment"} · {entity.Reference.DisplayName}"
                : "No queue or subscription selected in Explorer";

    public int MonitorIntervalSeconds
    {
        get => _monitorIntervalSeconds;
        set => SetProperty(ref _monitorIntervalSeconds, Math.Clamp(value, 15, 86_400));
    }

    public string MonitorStatus
    {
        get => _monitorStatus;
        private set => SetProperty(ref _monitorStatus, value);
    }

    public string MonitorAlert
    {
        get => _monitorAlert;
        private set
        {
            if (SetProperty(ref _monitorAlert, value))
            {
                OnPropertyChanged(nameof(HasMonitorAlert));
            }
        }
    }

    public bool HasMonitorAlert => !string.IsNullOrWhiteSpace(MonitorAlert);

    public string DraftBody
    {
        get => _draftBody;
        set
        {
            if (SetProperty(ref _draftBody, value))
            {
                OnPropertyChanged(nameof(DraftSizeText));
            }
        }
    }

    public MessageBodyFormat DraftBodyFormat
    {
        get => _draftBodyFormat;
        set
        {
            if (SetProperty(ref _draftBodyFormat, value))
            {
                OnPropertyChanged(nameof(DraftSizeText));
            }
        }
    }

    public string DraftMessageId { get => _draftMessageId; set => SetProperty(ref _draftMessageId, value); }
    public string DraftCorrelationId { get => _draftCorrelationId; set => SetProperty(ref _draftCorrelationId, value); }
    public string DraftSubject { get => _draftSubject; set => SetProperty(ref _draftSubject, value); }
    public string DraftContentType { get => _draftContentType; set => SetProperty(ref _draftContentType, value); }
    public string DraftSessionId { get => _draftSessionId; set => SetProperty(ref _draftSessionId, value); }
    public string DraftTo { get => _draftTo; set => SetProperty(ref _draftTo, value); }
    public string DraftReplyTo { get => _draftReplyTo; set => SetProperty(ref _draftReplyTo, value); }
    public string DraftReplyToSessionId { get => _draftReplyToSessionId; set => SetProperty(ref _draftReplyToSessionId, value); }
    public string DraftPartitionKey { get => _draftPartitionKey; set => SetProperty(ref _draftPartitionKey, value); }
    public string DraftTransactionPartitionKey { get => _draftTransactionPartitionKey; set => SetProperty(ref _draftTransactionPartitionKey, value); }
    public string DraftScheduledEnqueueTime { get => _draftScheduledEnqueueTime; set => SetProperty(ref _draftScheduledEnqueueTime, value); }
    public string DraftTimeToLiveSeconds { get => _draftTimeToLiveSeconds; set => SetProperty(ref _draftTimeToLiveSeconds, value); }
    public string DraftApplicationProperties { get => _draftApplicationProperties; set => SetProperty(ref _draftApplicationProperties, value); }

    public string DraftOriginNotice
    {
        get => _draftOriginNotice;
        private set => SetProperty(ref _draftOriginNotice, value);
    }

    public bool HasDraftEnvironmentMismatch =>
        IsConnected && _draftProfileId != _workspace.ConnectedProfileId;

    public string DraftEnvironmentWarning => HasDraftEnvironmentMismatch
        ? _draftProfileId.HasValue
            ? $"This draft belongs to '{_draftProfileName ?? "another environment"}'. Reconnect that environment or start a new message before choosing a destination."
            : "This draft is not pinned to an environment. Start a new message before choosing a destination."
        : string.Empty;

    public string DraftSizeText
    {
        get
        {
            try
            {
                var length = new EditableMessageBody(DraftBody, DraftBodyFormat).GetBytes().Length;
                return $"{length:N0} bytes";
            }
            catch
            {
                return "Invalid body encoding";
            }
        }
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await RunOperationAsync("Loading environments", async token =>
        {
            await ReloadProfilesAsync(token).ConfigureAwait(true);
            StatusText = Profiles.Count == 0
                ? "Add your first environment to begin"
                : "Choose an environment and connect";
        }, cancellationToken).ConfigureAwait(true);
    }

    private async Task AddEnvironmentAsync(CancellationToken cancellationToken)
    {
        var result = await _dialogs.EditProfileAsync(null, cancellationToken).ConfigureAwait(true);
        if (result is null)
        {
            return;
        }

        await SaveProfileAsync(result, cancellationToken).ConfigureAwait(true);
        await ReloadProfilesAsync(cancellationToken, result.Profile.Id).ConfigureAwait(true);
        AddActivity("Success", "Environment added", $"{result.Profile.Name} · {result.Profile.EnvironmentDisplayName}");
        StatusText = $"Environment '{result.Profile.Name}' saved";
    }

    private async Task EditEnvironmentAsync(CancellationToken cancellationToken)
    {
        var selected = SelectedProfile ?? throw new InvalidOperationException("Select an environment first.");
        var result = await _dialogs.EditProfileAsync(selected.Profile, cancellationToken).ConfigureAwait(true);
        if (result is null)
        {
            return;
        }

        var secretKey = ProfileSecretKey.ConnectionString(result.Profile.Id);
        string? removedConnectionString = null;
        var removesConnectionString = selected.Profile.Authentication.Kind == AuthenticationKind.ConnectionString &&
                                      result.Profile.Authentication.Kind == AuthenticationKind.EntraId;
        if (removesConnectionString)
        {
            removedConnectionString = await _secretVault.RetrieveAsync(secretKey, cancellationToken)
                .ConfigureAwait(true);
            if (removedConnectionString is not null)
            {
                await _secretVault.RemoveAsync(secretKey, cancellationToken).ConfigureAwait(true);
            }
        }

        try
        {
            await SaveProfileAsync(result, cancellationToken).ConfigureAwait(true);
        }
        catch
        {
            if (removedConnectionString is not null)
            {
                await _secretVault.StoreAsync(secretKey, removedConnectionString, CancellationToken.None)
                    .ConfigureAwait(true);
            }
            throw;
        }

        if (_writeUnlockProfileId == result.Profile.Id)
        {
            await StopWriteUnlockTimerAsync().ConfigureAwait(true);
        }
        await StopMonitorForConfigurationChangeAsync().ConfigureAwait(true);
        InvalidateProfileArtifacts(result.Profile.Id, "Environment configuration changed; the previous draft is no longer sendable.");

        if (_workspace.ConnectedProfileId == result.Profile.Id)
        {
            await _workspace.DisconnectAsync(cancellationToken).ConfigureAwait(true);
            ClearConnectedState();
        }

        await ReloadProfilesAsync(cancellationToken, result.Profile.Id).ConfigureAwait(true);
        AddActivity("Success", "Environment updated", result.Profile.Name);
    }

    private async Task SaveProfileAsync(ProfileEditorResult result, CancellationToken cancellationToken)
    {
        var secretKey = ProfileSecretKey.ConnectionString(result.Profile.Id);
        string? previousConnectionString = null;
        if (result.ReplacesConnectionString && result.ConnectionString is not null)
        {
            previousConnectionString = await _secretVault.RetrieveAsync(secretKey, cancellationToken)
                .ConfigureAwait(true);
            await _secretVault.StoreAsync(
                secretKey,
                result.ConnectionString,
                cancellationToken).ConfigureAwait(true);
        }

        try
        {
            await _profileRepository.UpsertAsync(result.Profile, cancellationToken).ConfigureAwait(true);
            await _profileRepository.SetSelectedProfileIdAsync(result.Profile.Id, cancellationToken).ConfigureAwait(true);
        }
        catch
        {
            if (result.ReplacesConnectionString)
            {
                if (previousConnectionString is null)
                {
                    await _secretVault.RemoveAsync(secretKey, CancellationToken.None).ConfigureAwait(true);
                }
                else
                {
                    await _secretVault.StoreAsync(secretKey, previousConnectionString, CancellationToken.None)
                        .ConfigureAwait(true);
                }
            }
            throw;
        }
    }

    private async Task DeleteEnvironmentAsync(CancellationToken cancellationToken)
    {
        var selected = SelectedProfile ?? throw new InvalidOperationException("Select an environment first.");
        var confirmed = await _dialogs.ConfirmAsync(
            "Delete environment",
            $"Remove '{selected.Name}' and its locally encrypted credential? Azure resources are not changed.",
            isDangerous: true,
            requiredText: selected.Name,
            cancellationToken: cancellationToken).ConfigureAwait(true);
        if (!confirmed)
        {
            return;
        }

        if (_workspace.ConnectedProfileId == selected.Id)
        {
            await _workspace.DisconnectAsync(cancellationToken).ConfigureAwait(true);
            ClearConnectedState();
        }

        var secretKey = ProfileSecretKey.ConnectionString(selected.Id);
        var connectionString = await _secretVault.RetrieveAsync(secretKey, cancellationToken)
            .ConfigureAwait(true);
        if (connectionString is not null)
        {
            await _secretVault.RemoveAsync(secretKey, cancellationToken).ConfigureAwait(true);
        }

        try
        {
            if (!await _profileRepository.DeleteAsync(selected.Id, cancellationToken).ConfigureAwait(true))
            {
                throw new InvalidOperationException("The environment no longer exists.");
            }
        }
        catch
        {
            if (connectionString is not null)
            {
                await _secretVault.StoreAsync(secretKey, connectionString, CancellationToken.None)
                    .ConfigureAwait(true);
            }
            throw;
        }
        if (_writeUnlockProfileId == selected.Id)
        {
            await StopWriteUnlockTimerAsync().ConfigureAwait(true);
        }
        await StopMonitorForConfigurationChangeAsync().ConfigureAwait(true);
        InvalidateProfileArtifacts(selected.Id, "Its environment was removed; the previous draft is no longer sendable.");
        AddActivity("Warning", "Environment removed", selected.Name);
        await ReloadProfilesAsync(cancellationToken).ConfigureAwait(true);
    }

    private async Task ReloadProfilesAsync(CancellationToken cancellationToken, Guid? selectId = null)
    {
        var profiles = await _profileRepository.ListAsync(cancellationToken).ConfigureAwait(true);
        var selectedId = selectId ?? await _profileRepository.GetSelectedProfileIdAsync(cancellationToken)
            .ConfigureAwait(true);

        Profiles.Clear();
        foreach (var profile in profiles)
        {
            Profiles.Add(new ProfileItemViewModel(profile));
        }
        SelectedProfile = Profiles.FirstOrDefault(item => item.Id == selectedId) ?? Profiles.FirstOrDefault();
        UpdateProfileConnectionStates();
        RefreshDeadLetterEnvironmentFilters();
        NotifyCommandStates();
    }

    private async Task ConnectSelectedAsync(CancellationToken cancellationToken)
    {
        var selected = SelectedProfile ?? throw new InvalidOperationException("Select an environment first.");
        await ConnectProfileAsync(selected, selected.Profile, loadTopology: true, cancellationToken).ConfigureAwait(true);
        await _profileRepository.SetSelectedProfileIdAsync(selected.Id, cancellationToken).ConfigureAwait(true);
        StatusText = $"Connected to {selected.Name}";
        AddActivity("Success", "Connected", $"{selected.Name} · {ConnectedNamespace}");
    }

    private async Task ConnectProfileAsync(
        ProfileItemViewModel item,
        ServiceBusProfile connectionProfile,
        bool loadTopology,
        CancellationToken cancellationToken)
    {
        var previousConnectedProfileId = _connectedProfile?.Id;
        ServiceBusTopology? topology = null;
        try
        {
            await _workspace.ConnectAsync(connectionProfile, cancellationToken).ConfigureAwait(true);
            if (loadTopology)
            {
                topology = await _workspace.GetTopologyAsync(forceRefresh: true, cancellationToken)
                    .ConfigureAwait(true);
            }
        }
        catch
        {
            if (_workspace.ConnectionState == WorkspaceConnectionState.Connected)
            {
                try
                {
                    await _workspace.DisconnectAsync(CancellationToken.None).ConfigureAwait(true);
                }
                catch
                {
                    // Preserve the original connection/topology error for the operator.
                }
            }
            ClearConnectedState();
            throw;
        }

        _connectedProfile = connectionProfile;
        SelectedProfile = item;
        var profileChanged = previousConnectedProfileId.HasValue &&
                             previousConnectedProfileId.Value != connectionProfile.Id;
        if (profileChanged)
        {
            Messages.Clear();
            SelectedMessage = null;
            SelectedDestination = null;
        }
        NotifyConnectionState();

        if (topology is not null)
        {
            ApplyTopology(topology, preserveDestination: !profileChanged);
        }
    }

    private async Task RefreshTopologyAsync(CancellationToken cancellationToken)
    {
        var topology = await _workspace.GetTopologyAsync(forceRefresh: true, cancellationToken)
            .ConfigureAwait(true);
        ApplyTopology(topology);
        StatusText = "Topology and runtime counters refreshed";
        AddActivity("Info", "Topology refreshed", $"{QueueCount} queues · {TopicCount} topics");
    }

    private void ApplyTopology(ServiceBusTopology topology, bool preserveDestination = true)
    {
        var previousDestination = preserveDestination ? SelectedDestination?.Reference : null;
        _topology = topology;
        _lastUpdated = topology.FetchedAt;
        _allEntities.Clear();

        foreach (var queue in topology.Queues)
        {
            _allEntities.Add(new EntityItemViewModel(
                queue.Reference,
                queue.Runtime,
                queue.Status,
                queue.RequiresSession,
                indent: 0));
        }

        foreach (var topic in topology.Topics)
        {
            _allEntities.Add(new EntityItemViewModel(
                topic.Reference,
                topic.Runtime,
                topic.Status,
                requiresSession: false,
                indent: 0));
            foreach (var subscription in topic.Subscriptions)
            {
                _allEntities.Add(new EntityItemViewModel(
                    subscription.Reference,
                    subscription.Runtime,
                    subscription.Status,
                    subscription.RequiresSession,
                    indent: 1));
            }
        }

        Destinations.Clear();
        foreach (var destination in topology.SendDestinations.OrderBy(item => item.DisplayName, StringComparer.OrdinalIgnoreCase))
        {
            Destinations.Add(new DestinationItemViewModel(destination));
        }
        SelectedDestination = previousDestination is null
            ? null
            : Destinations.FirstOrDefault(item => item.Reference == previousDestination);
        ApplyEntityFilter();
        NotifyStatistics();
    }

    private void ApplyEntityFilter()
    {
        var query = SearchText.Trim();
        var selection = SelectedEntity?.Reference;
        Entities.Clear();
        foreach (var item in _allEntities.Where(item =>
                     query.Length == 0 ||
                     item.Reference.DisplayName.Contains(query, StringComparison.OrdinalIgnoreCase) ||
                     item.KindLabel.Contains(query, StringComparison.OrdinalIgnoreCase)))
        {
            Entities.Add(item);
        }
        SelectedEntity = selection is null
            ? Entities.FirstOrDefault()
            : Entities.FirstOrDefault(item => item.Reference == selection) ?? Entities.FirstOrDefault();
    }

    private void RefreshDeadLetterEnvironmentFilters()
    {
        var selectedProfileId = SelectedDeadLetterEnvironmentFilter?.ProfileId;

        DeadLetterEnvironmentFilters.Clear();
        DeadLetterEnvironmentFilters.Add(new DeadLetterEnvironmentFilterItemViewModel(
            null,
            "All environments",
            "ALL",
            "#91A5BD"));
        foreach (var profile in Profiles)
        {
            DeadLetterEnvironmentFilters.Add(new DeadLetterEnvironmentFilterItemViewModel(
                profile.Id,
                profile.Name,
                profile.EnvironmentLabel,
                profile.EnvironmentColor));
        }

        SelectedDeadLetterEnvironmentFilter = DeadLetterEnvironmentFilters
            .FirstOrDefault(filter => filter.ProfileId == selectedProfileId)
            ?? DeadLetterEnvironmentFilters[0];
    }

    private void ApplyDeadLetterEnvironmentFilter()
    {
        var filter = SelectedDeadLetterEnvironmentFilter;
        FilteredDeadLetterSources.Clear();
        foreach (var source in DeadLetterSources.Where(source => filter?.Matches(source) != false))
        {
            FilteredDeadLetterSources.Add(source);
        }

        SelectedDlqSource = _preferredDlqSourceProfileId.HasValue &&
                            _preferredDlqSourceEntity is not null &&
                            _preferredDlqSourceSubQueue.HasValue
            ? FilteredDeadLetterSources.FirstOrDefault(item =>
                item.ProfileId == _preferredDlqSourceProfileId.Value &&
                item.Entity == _preferredDlqSourceEntity &&
                item.Snapshot.SubQueue == _preferredDlqSourceSubQueue.Value)
            : null;
        OnPropertyChanged(nameof(VisibleDlqSourceCount));
        OnPropertyChanged(nameof(VisibleDlqSourceRowCount));
    }

    private async Task ScanCurrentEnvironmentAsync(CancellationToken cancellationToken)
    {
        var profile = GetConnectedProfileItem();

        _lastDlqMeasurements.Clear();
        var snapshot = await _workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All, cancellationToken)
            .ConfigureAwait(true);
        CaptureDlqMeasurements(profile.Id, snapshot);
        _lastDlqScanHadFailures = snapshot.HasFailures;
        UpdateDeadLetterRows(profile, snapshot, replaceExisting: true);
        var failedSources = snapshot.Entities
            .Where(entity => !entity.IsSuccessful)
            .Select(entity => entity.Entity)
            .Distinct()
            .Count();
        StatusText = snapshot.HasFailures
            ? $"Partial scan in {profile.Name} · {snapshot.TotalCount:N0} known messages · {failedSources} source errors"
            : $"Found {snapshot.TotalCount:N0} dead-letter messages in {profile.Name}";
        AddActivity(
            snapshot.HasFailures ? "Error" : snapshot.TotalCount > 0 ? "Warning" : "Success",
            snapshot.HasFailures ? "Partial DLQ scan" : "DLQ scan",
            $"{profile.Name} · {snapshot.TotalCount:N0} known messages · {failedSources} source errors");
    }

    private async Task ScanAllEnvironmentsAsync(CancellationToken cancellationToken)
    {
        var selectedProfileId = SelectedDlqSource?.ProfileId;
        var selectedEntity = SelectedDlqSource?.Entity;
        var selectedSubQueue = SelectedDlqSource?.Snapshot.SubQueue;
        var connectedProfileBeforeScan = _connectedProfile;
        _lastDlqMeasurements.Clear();
        DeadLetterSources.Clear();
        ApplyDeadLetterEnvironmentFilter();
        var preferredProfileId = connectedProfileBeforeScan?.Id ?? SelectedProfile?.Id;
        var scanFailures = 0;
        var successfulEnvironments = 0;
        var partialFailures = 0;
        var restoreFailed = false;
        var total = 0L;
        _lastDlqScanHadFailures = false;

        try
        {
            foreach (var profile in Profiles.ToArray())
            {
                cancellationToken.ThrowIfCancellationRequested();
                StatusText = $"Scanning {profile.Name}…";
                try
                {
                    var readOnlyProfile = profile.Profile with { AccessMode = ProfileAccessMode.ReadOnly };
                    await ConnectProfileAsync(profile, readOnlyProfile, loadTopology: true, cancellationToken)
                        .ConfigureAwait(true);
                    var snapshot = await _workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All, cancellationToken)
                        .ConfigureAwait(true);
                    CaptureDlqMeasurements(profile.Id, snapshot);
                    UpdateDeadLetterRows(profile, snapshot, replaceExisting: false);
                    total = checked(total + snapshot.TotalCount);
                    successfulEnvironments++;
                    if (snapshot.HasFailures)
                    {
                        partialFailures++;
                        _lastDlqScanHadFailures = true;
                    }
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    scanFailures++;
                    _lastDlqScanHadFailures = true;
                    AddActivity("Error", "Environment scan failed", $"{profile.Name} · {SanitizeException(exception)}");
                }
            }
        }
        finally
        {
            if (!_isDisposed)
            {
                var preferred = Profiles.FirstOrDefault(profile => profile.Id == preferredProfileId)
                    ?? Profiles.FirstOrDefault();
                if (preferred is not null)
                {
                    var connectionProfile = connectedProfileBeforeScan?.Id == preferred.Id
                        ? connectedProfileBeforeScan
                        : preferred.Profile;
                    if (connectionProfile.CanWrite &&
                        _writeUnlockProfileId == connectionProfile.Id &&
                        !IsTemporaryWriteUnlockActive)
                    {
                        connectionProfile = preferred.Profile;
                    }
                    using var restoreCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                    try
                    {
                        await ConnectProfileAsync(
                                preferred,
                                connectionProfile,
                                loadTopology: true,
                                restoreCancellation.Token)
                            .ConfigureAwait(true);
                    }
                    catch (Exception exception)
                    {
                        restoreFailed = true;
                        _lastDlqScanHadFailures = true;
                        AddActivity(
                            "Error",
                            "Environment restore failed",
                            $"{preferred.Name} · {SanitizeException(exception)}");
                    }
                }
            }
            SortDeadLetterSources(selectedProfileId, selectedEntity, selectedSubQueue);
        }

        StatusText = scanFailures == 0 && partialFailures == 0 && !restoreFailed
            ? $"All environments scanned · {total:N0} dead-letter messages"
            : $"Partial global scan · {total:N0} known messages · {scanFailures} scan errors · {partialFailures} environments with source errors · restore {(restoreFailed ? "failed" : "ok")}";
        AddActivity(
            scanFailures == 0 && partialFailures == 0 && !restoreFailed ? (total > 0 ? "Warning" : "Success") : "Error",
            scanFailures == 0 && partialFailures == 0 && !restoreFailed ? "Global DLQ scan" : "Partial global DLQ scan",
            $"{successfulEnvironments}/{Profiles.Count} scanned · {partialFailures} partial · restore {(restoreFailed ? "failed" : "ok")} · {total:N0} known messages");
        NotifyStatistics();
    }

    private void UpdateDeadLetterRows(
        ProfileItemViewModel profile,
        DeadLetterSnapshot snapshot,
        bool replaceExisting,
        ServiceBusEntityReference? replaceEntity = null)
    {
        var selectedProfileId = SelectedDlqSource?.ProfileId;
        var selectedEntity = SelectedDlqSource?.Entity;
        var selectedSubQueue = SelectedDlqSource?.Snapshot.SubQueue;

        if (replaceExisting)
        {
            foreach (var existing in DeadLetterSources.Where(item => item.ProfileId == profile.Id).ToArray())
            {
                DeadLetterSources.Remove(existing);
            }
        }
        else if (replaceEntity is not null)
        {
            foreach (var existing in DeadLetterSources
                         .Where(item => item.ProfileId == profile.Id && item.Entity == replaceEntity)
                         .ToArray())
            {
                DeadLetterSources.Remove(existing);
            }
        }

        var previousCounts = new Dictionary<string, long?>(StringComparer.Ordinal);
        foreach (var entity in snapshot.Entities)
        {
            var key = $"{profile.Id:N}|{entity.Entity.Path}|{entity.SubQueue}";
            previousCounts[key] = _previousDlqCounts.TryGetValue(key, out var previousValue)
                ? previousValue
                : null;
            if (entity.IsSuccessful && entity.Count.HasValue)
            {
                _previousDlqCounts[key] = entity.Count.Value;
            }
        }

        foreach (var entity in snapshot.Entities.Where(item => item.Count > 0 || !item.IsSuccessful))
        {
            var key = $"{profile.Id:N}|{entity.Entity.Path}|{entity.SubQueue}";
            var withHistory = new DeadLetterEntitySnapshot(
                entity.Entity,
                entity.Count,
                previousCounts[key],
                entity.Error,
                entity.SubQueue);
            DeadLetterSources.Add(new DlqSourceItemViewModel(
                profile.Id,
                profile.Name,
                profile.EnvironmentLabel,
                profile.EnvironmentColor,
                withHistory));
        }

        SortDeadLetterSources(selectedProfileId, selectedEntity, selectedSubQueue);
        NotifyStatistics();
    }

    private void CaptureDlqMeasurements(Guid profileId, DeadLetterSnapshot snapshot)
    {
        foreach (var entity in snapshot.Entities.Where(item => item.IsSuccessful && item.Count.HasValue))
        {
            var key = $"{profileId:N}|{entity.Entity.Path}|{entity.SubQueue}";
            _lastDlqMeasurements[key] = entity.Count!.Value;
        }
    }

    private void SortDeadLetterSources(
        Guid? selectedProfileId,
        ServiceBusEntityReference? selectedEntity,
        ServiceBusSubQueue? selectedSubQueue)
    {
        if (selectedProfileId.HasValue && selectedEntity is not null && selectedSubQueue.HasValue)
        {
            _preferredDlqSourceProfileId = selectedProfileId;
            _preferredDlqSourceEntity = selectedEntity;
            _preferredDlqSourceSubQueue = selectedSubQueue;
        }

        var sorted = DeadLetterSources
            .OrderBy(item => item.ProfileName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.EnvironmentLabel, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.IsSubscription)
            .ThenBy(item => item.ParentTopicName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.EntityName, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.Snapshot.SubQueue)
            .ToArray();
        DeadLetterSources.Clear();
        foreach (var item in sorted)
        {
            DeadLetterSources.Add(item);
        }

        ApplyDeadLetterEnvironmentFilter();
    }

    private Task BrowseSelectedEntityAsync(ServiceBusSubQueue subQueue, CancellationToken cancellationToken)
    {
        var entity = SelectedEntity ?? throw new InvalidOperationException("Select a queue or subscription first.");
        var profile = GetConnectedProfileItem();
        return BrowseAsync(profile, entity.Reference, subQueue, cancellationToken);
    }

    private async Task BrowseSelectedDlqSourceAsync(CancellationToken cancellationToken)
    {
        var source = SelectedDlqSource ?? throw new InvalidOperationException("Select a DLQ source first.");
        var profile = Profiles.FirstOrDefault(item => item.Id == source.ProfileId)
            ?? throw new InvalidOperationException("The source environment no longer exists.");
        await BrowseAsync(profile, source.Entity, source.Snapshot.SubQueue, cancellationToken)
            .ConfigureAwait(true);
    }

    private async Task BrowseAsync(
        ProfileItemViewModel profile,
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue,
        CancellationToken cancellationToken)
    {
        if (_workspace.ConnectedProfileId != profile.Id)
        {
            await ConnectProfileAsync(profile, profile.Profile, loadTopology: true, cancellationToken)
                .ConfigureAwait(true);
        }

        var messages = await _workspace.BrowseMessagesAsync(
                new BrowseMessagesRequest(source, subQueue, maxMessages: 10),
                cancellationToken)
            .ConfigureAwait(true);
        Messages.Clear();
        foreach (var message in messages)
        {
            Messages.Add(new MessageItemViewModel(message));
        }
        SelectedMessage = Messages.FirstOrDefault();
        MessageListTitle = $"{profile.Name} · {source.DisplayName} · {FormatSubQueue(subQueue)}";
        NavigateTo(NavigationPage.DeadLetters);
        StatusText = $"Peeked {messages.Count:N0} messages without acquiring locks";
        AddActivity("Info", "Peek", $"{source.DisplayName} · {FormatSubQueue(subQueue)} · {messages.Count:N0} messages");
    }

    private void NewMessage()
    {
        _draftSourceMessage = null;
        BindDraftToConnectedEnvironment();
        DraftBody = "{\n  \"event\": \"example\"\n}";
        DraftBodyFormat = MessageBodyFormat.Json;
        DraftMessageId = Guid.NewGuid().ToString("N");
        DraftCorrelationId = string.Empty;
        DraftSubject = string.Empty;
        DraftContentType = "application/json";
        DraftSessionId = string.Empty;
        DraftTo = string.Empty;
        DraftReplyTo = string.Empty;
        DraftReplyToSessionId = string.Empty;
        DraftPartitionKey = string.Empty;
        DraftTransactionPartitionKey = string.Empty;
        DraftScheduledEnqueueTime = string.Empty;
        DraftTimeToLiveSeconds = string.Empty;
        DraftApplicationProperties = "{}";
        DraftOriginNotice = "New message · destination is pinned to the connected environment";
        NavigateTo(NavigationPage.Composer);
    }

    private void OpenSelectedMessageAsDraft()
    {
        var selected = SelectedMessage?.Message ?? throw new InvalidOperationException("Select a message first.");
        var draft = selected.CreateDraft();
        _draftSourceMessage = selected;
        BindDraftToConnectedEnvironment();
        DraftBody = draft.Body.Content;
        DraftBodyFormat = draft.Body.Format;
        DraftMessageId = draft.Properties.MessageId ?? Guid.NewGuid().ToString("N");
        DraftCorrelationId = draft.Properties.CorrelationId ?? string.Empty;
        DraftSubject = draft.Properties.Subject ?? string.Empty;
        DraftContentType = draft.Properties.ContentType ?? string.Empty;
        DraftSessionId = draft.Properties.SessionId ?? string.Empty;
        DraftTo = draft.Properties.To ?? string.Empty;
        DraftReplyTo = draft.Properties.ReplyTo ?? string.Empty;
        DraftReplyToSessionId = draft.Properties.ReplyToSessionId ?? string.Empty;
        DraftPartitionKey = draft.Properties.PartitionKey ?? string.Empty;
        DraftTransactionPartitionKey = draft.Properties.TransactionPartitionKey ?? string.Empty;
        DraftScheduledEnqueueTime = draft.Properties.ScheduledEnqueueTime?.ToString("O", CultureInfo.InvariantCulture)
            ?? string.Empty;
        DraftTimeToLiveSeconds = draft.Properties.TimeToLive?.TotalSeconds.ToString("0.###") ?? string.Empty;
        DraftApplicationProperties = ApplicationPropertiesJson.Serialize(draft.ApplicationProperties);

        var destination = selected.Source.Kind == ServiceBusEntityKind.Subscription
            ? ServiceBusEntityReference.Topic(selected.Source.TopicName!)
            : ServiceBusEntityReference.Queue(selected.Source.Name);
        SelectedDestination = Destinations.FirstOrDefault(item => item.Reference == destination);
        DraftOriginNotice = selected.IsDeadLetter
            ? "DLQ draft · resend sends a copy. Original remains in DLQ."
            : "Peeked active-message draft · Send creates a new copy and leaves the original message unchanged.";
        if (destination.Kind == ServiceBusEntityKind.Topic)
        {
            DraftOriginNotice += " The suggested destination is a topic and may fan out to every matching subscription.";
        }
        NavigateTo(NavigationPage.Composer);
    }

    private async Task SendDraftAsync(CancellationToken cancellationToken)
    {
        var destination = SelectedDestination
            ?? throw new InvalidOperationException("Choose a queue or topic destination.");
        var profile = _connectedProfile ?? throw new InvalidOperationException("Connect to an environment first.");
        if (HasDraftEnvironmentMismatch)
        {
            throw new InvalidOperationException(
                "This draft belongs to a different environment. Reconnect it or start a new message before sending.");
        }
        var draft = BuildDraft();

        var warning = _draftSourceMessage switch
        {
            { IsDeadLetter: true } =>
                "Send the edited copy? The original message stays in DLQ.",
            not null =>
                "Send an edited copy of the peeked active message? The original message is not changed.",
            _ => "Send this new message to the selected entity?"
        };
        if (destination.Reference.Kind == ServiceBusEntityKind.Topic)
        {
            warning += "\n\nThe selected destination is a topic. This copy may fan out to every matching subscription.";
        }
        if (_draftSourceMessage is not null)
        {
            warning += "\n\nThe original MessageId is currently preserved. With duplicate detection enabled, Azure may accept the send but suppress the duplicate; change MessageId when a distinct delivery is required.";
        }
        var confirmed = await _dialogs.ConfirmAsync(
            $"Send to {destination.Name}",
            $"Environment: {profile.Name}\nDestination: {destination.Reference.DisplayName}\nMessageId: {draft.Properties.MessageId}\n\n{warning}",
            isDangerous: profile.Environment == EnvironmentKind.Production,
            requiredText: profile.Environment == EnvironmentKind.Production ? destination.Reference.Name : null,
            cancellationToken: cancellationToken)
            .ConfigureAwait(true);
        if (!confirmed)
        {
            return;
        }

        if (!CanWrite)
        {
            throw new InvalidOperationException(
                "Write access expired while the confirmation was open. Unlock writes again and review the send.");
        }

        if (_draftSourceMessage is not null && _draftSourceMessage.IsDeadLetter)
        {
            await _workspace.ResubmitDeadLetterAsync(
                new ResubmitDeadLetterRequest(
                    _draftSourceMessage.Source,
                    _draftSourceMessage.SequenceNumber,
                    destination.Reference,
                    draft,
                    DeadLetterDisposition.KeepOriginal),
                cancellationToken).ConfigureAwait(true);
            StatusText = "Copy accepted by Service Bus · original remains in DLQ";
            AddActivity("Success", "DLQ copy send accepted", $"{profile.Name} · {destination.Reference.DisplayName}");
        }
        else
        {
            await _workspace.SendMessageAsync(
                new SendMessageRequest(destination.Reference, draft),
                cancellationToken).ConfigureAwait(true);
            StatusText = "Message accepted by Service Bus";
            AddActivity("Success", "Message send accepted", $"{profile.Name} · {destination.Reference.DisplayName}");
        }
    }

    private MessageDraft BuildDraft()
    {
        TimeSpan? timeToLive = null;
        if (!string.IsNullOrWhiteSpace(DraftTimeToLiveSeconds))
        {
            if (!double.TryParse(DraftTimeToLiveSeconds, out var seconds) || seconds <= 0)
            {
                throw new InvalidOperationException("TTL must be a positive number of seconds.");
            }
            timeToLive = TimeSpan.FromSeconds(seconds);
        }

        DateTimeOffset? scheduledEnqueueTime = null;
        if (!string.IsNullOrWhiteSpace(DraftScheduledEnqueueTime))
        {
            if (!DateTimeOffset.TryParse(
                    DraftScheduledEnqueueTime,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.AssumeUniversal,
                    out var scheduledAt))
            {
                throw new InvalidOperationException(
                    "Scheduled enqueue time must be an ISO 8601 timestamp, for example 2026-08-11T14:30:00Z.");
            }
            scheduledEnqueueTime = scheduledAt;
        }

        IReadOnlyList<MessageApplicationProperty> applicationProperties;
        try
        {
            applicationProperties = ApplicationPropertiesJson.Deserialize(DraftApplicationProperties);
        }
        catch (JsonException exception)
        {
            throw new InvalidOperationException("Application properties must be valid typed JSON.", exception);
        }

        var sourceProperties = _draftSourceMessage?.Properties ?? EditableMessageProperties.Empty;
        var properties = sourceProperties with
        {
            MessageId = NullIfWhiteSpace(DraftMessageId) ?? Guid.NewGuid().ToString("N"),
            CorrelationId = NullIfWhiteSpace(DraftCorrelationId),
            ContentType = NullIfWhiteSpace(DraftContentType),
            Subject = NullIfWhiteSpace(DraftSubject),
            To = NullIfWhiteSpace(DraftTo),
            ReplyTo = NullIfWhiteSpace(DraftReplyTo),
            SessionId = NullIfWhiteSpace(DraftSessionId),
            ReplyToSessionId = NullIfWhiteSpace(DraftReplyToSessionId),
            PartitionKey = NullIfWhiteSpace(DraftPartitionKey),
            TransactionPartitionKey = NullIfWhiteSpace(DraftTransactionPartitionKey),
            TimeToLive = timeToLive,
            ScheduledEnqueueTime = scheduledEnqueueTime
        };
        return new MessageDraft(
            new EditableMessageBody(DraftBody ?? string.Empty, DraftBodyFormat),
            properties,
            applicationProperties);
    }

    private async Task UnlockWritesAsync(CancellationToken cancellationToken)
    {
        var selected = GetConnectedProfileItem();
        var confirmed = await _dialogs.ConfirmAsync(
            "Temporarily unlock writes",
            "Write access will be enabled for this local session for 10 minutes. Azure RBAC/SAS permissions still apply.",
            isDangerous: selected.IsProduction,
            requiredText: selected.IsProduction ? selected.Name : null,
            cancellationToken: cancellationToken).ConfigureAwait(true);
        if (!confirmed)
        {
            return;
        }

        var unlocked = selected.Profile with { AccessMode = ProfileAccessMode.ReadWrite };
        await StopWriteUnlockTimerAsync().ConfigureAwait(true);
        await ConnectProfileAsync(selected, unlocked, loadTopology: true, cancellationToken).ConfigureAwait(true);
        var expiresAt = DateTimeOffset.UtcNow.AddMinutes(10);
        _writeUnlockCancellation = new CancellationTokenSource();
        _writeUnlockProfileId = selected.Id;
        _writeUnlockExpiresAt = expiresAt;
        _writeUnlockTask = RelockAfterDelayAsync(selected.Id, expiresAt, _writeUnlockCancellation.Token);
        NotifyConnectionState();
        StatusText = "Write access unlocked for 10 minutes";
        AddActivity("Warning", "Writes unlocked", $"{selected.Name} · expires in 10 minutes");
    }

    private async Task RelockAfterDelayAsync(
        Guid profileId,
        DateTimeOffset expiresAt,
        CancellationToken cancellationToken)
    {
        try
        {
            var delay = expiresAt - DateTimeOffset.UtcNow;
            if (delay > TimeSpan.Zero)
            {
                await Task.Delay(delay, cancellationToken).ConfigureAwait(true);
            }
            NotifyConnectionState();
            await _workspaceGate.WaitAsync(cancellationToken).ConfigureAwait(true);
            try
            {
                var profile = Profiles.FirstOrDefault(item => item.Id == profileId);
                if (profile is not null && _workspace.ConnectedProfileId == profileId)
                {
                    await ConnectProfileAsync(profile, profile.Profile, loadTopology: false, cancellationToken)
                        .ConfigureAwait(true);
                    StatusText = "Temporary write access expired; environment is read-only";
                    AddActivity("Info", "Writes relocked", profile.Name);
                }
            }
            finally
            {
                _workspaceGate.Release();
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception exception)
        {
            ErrorText = SanitizeException(exception);
        }
        finally
        {
            if (_writeUnlockProfileId == profileId &&
                _writeUnlockExpiresAt == expiresAt &&
                (_workspace.ConnectedProfileId != profileId || _connectedProfile?.CanWrite != true))
            {
                ClearTemporaryWriteState();
            }
        }
    }

    private async Task StopWriteUnlockTimerAsync()
    {
        var cancellation = _writeUnlockCancellation;
        var task = _writeUnlockTask;
        cancellation?.Cancel();
        if (task is not null)
        {
            try
            {
                await task.ConfigureAwait(true);
            }
            catch (OperationCanceledException)
            {
            }
        }
        cancellation?.Dispose();
        if (ReferenceEquals(_writeUnlockCancellation, cancellation))
        {
            _writeUnlockCancellation = null;
            _writeUnlockTask = null;
        }
    }

    private async Task ToggleMonitorAsync(CancellationToken cancellationToken)
    {
        if (IsMonitoring)
        {
            var stoppedScope = _activeMonitorScope ?? MonitorScope;
            await StopMonitorLoopAsync().ConfigureAwait(true);
            _monitoredProfileId = null;
            _monitoredEntity = null;
            _activeMonitorScope = null;
            _activeMonitorTargetLabel = null;
            _hasMonitorBaseline = false;
            _monitorBaseline.Clear();
            IsMonitoring = false;
            MonitorStatus = "Monitor is stopped";
            MonitorAlert = string.Empty;
            AddActivity("Info", "Monitor stopped", stoppedScope);
            return;
        }

        cancellationToken.ThrowIfCancellationRequested();

        _monitoredProfileId = null;
        _monitoredEntity = null;
        if (MonitorScope == CurrentEnvironmentMonitorScope)
        {
            _monitoredProfileId = _workspace.ConnectedProfileId ?? SelectedProfile?.Id;
            if (!_monitoredProfileId.HasValue)
            {
                ErrorText = "Select an environment before starting the current-environment monitor.";
                return;
            }
        }
        else if (MonitorScope == SelectedSourceMonitorScope)
        {
            if (MonitorTargetChoice == DeadLettersMonitorTarget && SelectedDlqSource is { } dlqSource)
            {
                _monitoredProfileId = dlqSource.ProfileId;
                _monitoredEntity = dlqSource.Entity;
            }
            else if (MonitorTargetChoice == ExplorerMonitorTarget &&
                     SelectedEntity is { CanBrowse: true } entity)
            {
                var profile = GetConnectedProfileItem();
                _monitoredProfileId = profile.Id;
                _monitoredEntity = entity.Reference;
            }
            else
            {
                ErrorText = MonitorTargetChoice == DeadLettersMonitorTarget
                    ? "Choose a source in Dead letters, or switch the monitor target to Explorer selection."
                    : "Choose a queue or subscription in Explorer, or switch the monitor target to Dead letters selection.";
                return;
            }
        }

        ErrorText = string.Empty;
        MonitorAlert = string.Empty;
        _monitorCancellation = new CancellationTokenSource();
        _activeMonitorScope = MonitorScope;
        _activeMonitorIntervalSeconds = MonitorIntervalSeconds;
        _hasMonitorBaseline = false;
        _monitorBaseline.Clear();
        var target = _activeMonitorScope == AllEnvironmentsMonitorScope
            ? AllEnvironmentsMonitorScope
            : _monitoredEntity is null
                ? $"{Profiles.First(item => item.Id == _monitoredProfileId).Name} · all DLQs"
                : $"{Profiles.First(item => item.Id == _monitoredProfileId).Name} · {_monitoredEntity.DisplayName}";
        _activeMonitorTargetLabel = target;
        IsMonitoring = true;
        MonitorStatus = $"Monitoring {target} every {_activeMonitorIntervalSeconds} seconds";
        AddActivity("Success", "Monitor started", $"{target} · {_activeMonitorIntervalSeconds}s");
        _monitorTask = MonitorLoopAsync(_monitorCancellation.Token);
    }

    private async Task StopMonitorLoopAsync()
    {
        var cancellation = _monitorCancellation;
        var task = _monitorTask;
        cancellation?.Cancel();
        if (task is not null)
        {
            try
            {
                await task.ConfigureAwait(true);
            }
            catch (OperationCanceledException)
            {
            }
        }
        cancellation?.Dispose();
        if (ReferenceEquals(_monitorCancellation, cancellation))
        {
            _monitorCancellation = null;
            _monitorTask = null;
        }
    }

    private async Task StopMonitorForConfigurationChangeAsync()
    {
        if (!IsMonitoring)
        {
            return;
        }

        var stoppedScope = _activeMonitorScope ?? MonitorScope;
        await StopMonitorLoopAsync().ConfigureAwait(true);
        _monitoredProfileId = null;
        _monitoredEntity = null;
        _activeMonitorScope = null;
        _activeMonitorTargetLabel = null;
        _hasMonitorBaseline = false;
        _monitorBaseline.Clear();
        IsMonitoring = false;
        MonitorStatus = "Monitor stopped because an environment changed";
        MonitorAlert = string.Empty;
        AddActivity("Info", "Monitor stopped", $"{stoppedScope} · environment configuration changed");
    }

    private async Task MonitorLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (true)
            {
                var ranCheck = false;
                var checkComplete = false;
                if (!IsBusy &&
                    await _workspaceGate.WaitAsync(0, cancellationToken).ConfigureAwait(true))
                {
                    try
                    {
                        IsBusy = true;
                        ranCheck = true;
                        checkComplete = await RunMonitorCheckAsync(cancellationToken).ConfigureAwait(true);
                    }
                    finally
                    {
                        IsBusy = false;
                        _workspaceGate.Release();
                    }
                }
                MonitorStatus = !ranCheck
                    ? $"{_activeMonitorTargetLabel} · check skipped at {DateTimeOffset.Now:HH:mm:ss} · workspace busy"
                    : checkComplete
                    ? $"{_activeMonitorTargetLabel} · last check {DateTimeOffset.Now:HH:mm:ss} · next in {_activeMonitorIntervalSeconds}s"
                    : $"{_activeMonitorTargetLabel} · check incomplete at {DateTimeOffset.Now:HH:mm:ss} · previous baseline retained";
                await Task.Delay(
                        TimeSpan.FromSeconds(_activeMonitorIntervalSeconds),
                        cancellationToken)
                    .ConfigureAwait(true);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception exception)
        {
            MonitorStatus = "Monitor stopped after an error";
            MonitorAlert = SanitizeException(exception);
            AddActivity("Error", "Monitor failed", MonitorAlert);
            _monitoredProfileId = null;
            _monitoredEntity = null;
            _activeMonitorScope = null;
            _activeMonitorTargetLabel = null;
            _hasMonitorBaseline = false;
            _monitorBaseline.Clear();
            IsMonitoring = false;
        }
    }

    private async Task<bool> RunMonitorCheckAsync(CancellationToken cancellationToken)
    {
        long total;
        var isComplete = true;
        var activeScope = _activeMonitorScope ?? MonitorScope;
        if (activeScope == AllEnvironmentsMonitorScope)
        {
            await ScanAllEnvironmentsAsync(cancellationToken).ConfigureAwait(true);
            total = _lastDlqMeasurements.Values.Sum();
            isComplete = !_lastDlqScanHadFailures;
        }
        else if (activeScope == CurrentEnvironmentMonitorScope)
        {
            var monitoredProfileId = _monitoredProfileId
                ?? throw new InvalidOperationException("The monitored environment is no longer available.");
            var selected = Profiles.FirstOrDefault(profile => profile.Id == monitoredProfileId)
                ?? throw new InvalidOperationException("The monitored environment no longer exists.");
            if (_workspace.ConnectedProfileId != monitoredProfileId)
            {
                await ConnectProfileAsync(selected, selected.Profile, loadTopology: true, cancellationToken)
                    .ConfigureAwait(true);
            }
            await ScanCurrentEnvironmentAsync(cancellationToken).ConfigureAwait(true);
            total = _lastDlqMeasurements.Values.Sum();
            isComplete = !_lastDlqScanHadFailures;
        }
        else
        {
            var monitoredProfileId = _monitoredProfileId
                ?? throw new InvalidOperationException("The monitored environment is no longer available.");
            var monitoredEntity = _monitoredEntity
                ?? throw new InvalidOperationException("The monitored source is no longer available.");
            var profile = Profiles.FirstOrDefault(item => item.Id == monitoredProfileId)
                ?? throw new InvalidOperationException("The monitored environment no longer exists.");
            if (_workspace.ConnectedProfileId != profile.Id)
            {
                await ConnectProfileAsync(profile, profile.Profile, loadTopology: true, cancellationToken)
                    .ConfigureAwait(true);
            }
            var snapshot = await _workspace.GetDeadLetterSnapshotAsync(
                    DeadLetterMonitorScope.ForEntity(monitoredEntity),
                    cancellationToken)
                .ConfigureAwait(true);
            _lastDlqMeasurements.Clear();
            CaptureDlqMeasurements(profile.Id, snapshot);
            UpdateDeadLetterRows(profile, snapshot, replaceExisting: false, replaceEntity: monitoredEntity);
            total = _lastDlqMeasurements.Values.Sum();
            isComplete = !snapshot.HasFailures;
        }

        if (!isComplete)
        {
            MonitorAlert = $"DLQ check was incomplete at {DateTimeOffset.Now:HH:mm:ss}; known counts were not used as a new baseline.";
            AddActivity("Error", "Partial monitor check", MonitorAlert);
            return false;
        }

        var increases = _hasMonitorBaseline
            ? _lastDlqMeasurements
                .Select(measurement =>
                    measurement.Value - _monitorBaseline.GetValueOrDefault(measurement.Key))
                .Where(increase => increase > 0)
                .ToArray()
            : [];
        if (_hasMonitorBaseline && increases.Length > 0)
        {
            MonitorAlert = $"{increases.Length:N0} DLQ source(s) increased by {increases.Sum():N0}; total is {total:N0} at {DateTimeOffset.Now:HH:mm:ss}";
            AddActivity("Warning", "DLQ alert", MonitorAlert);
        }
        else
        {
            MonitorAlert = string.Empty;
        }
        _monitorBaseline.Clear();
        foreach (var measurement in _lastDlqMeasurements)
        {
            _monitorBaseline[measurement.Key] = measurement.Value;
        }
        _hasMonitorBaseline = true;
        return true;
    }

    private async Task RunWorkspaceOperationAsync(
        string operation,
        Func<CancellationToken, Task> action,
        CancellationToken cancellationToken)
    {
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
                cancellationToken)
            .ConfigureAwait(true);
    }

    private async Task RunOperationAsync(
        string operation,
        Func<CancellationToken, Task> action,
        CancellationToken cancellationToken)
    {
        IsBusy = true;
        ErrorText = string.Empty;
        StatusText = operation;
        try
        {
            await action(cancellationToken).ConfigureAwait(true);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            StatusText = "Operation cancelled";
        }
        catch (Exception exception)
        {
            ErrorText = SanitizeException(exception);
            StatusText = $"{operation} failed";
            AddActivity("Error", operation, ErrorText);
        }
        finally
        {
            IsBusy = false;
        }
    }

    private void ClearConnectedState()
    {
        _connectedProfile = null;
        ClearTemporaryWriteState();
        _topology = null;
        _allEntities.Clear();
        Entities.Clear();
        SelectedEntity = null;
        Destinations.Clear();
        Messages.Clear();
        SelectedMessage = null;
        SelectedDestination = null;
        NotifyConnectionState();
        NotifyStatistics();
    }

    private void ClearTemporaryWriteState()
    {
        _writeUnlockProfileId = null;
        _writeUnlockExpiresAt = null;
        NotifyConnectionState();
    }

    private void InvalidateProfileArtifacts(Guid profileId, string draftNotice)
    {
        foreach (var source in DeadLetterSources.Where(item => item.ProfileId == profileId).ToArray())
        {
            DeadLetterSources.Remove(source);
        }
        if (_preferredDlqSourceProfileId == profileId)
        {
            _preferredDlqSourceProfileId = null;
            _preferredDlqSourceEntity = null;
            _preferredDlqSourceSubQueue = null;
        }
        ApplyDeadLetterEnvironmentFilter();

        var keyPrefix = $"{profileId:N}|";
        foreach (var key in _previousDlqCounts.Keys.Where(key => key.StartsWith(keyPrefix, StringComparison.Ordinal)).ToArray())
        {
            _previousDlqCounts.Remove(key);
        }

        Messages.Clear();
        SelectedMessage = null;
        if (_draftProfileId == profileId)
        {
            _draftProfileId = null;
            _draftProfileName = null;
            _draftSourceMessage = null;
            SelectedDestination = null;
            DraftOriginNotice = draftNotice;
            OnPropertyChanged(nameof(HasDraftEnvironmentMismatch));
            OnPropertyChanged(nameof(DraftEnvironmentWarning));
            SendDraftCommand.NotifyCanExecuteChanged();
        }
        NotifyStatistics();
    }

    private void BindDraftToConnectedEnvironment()
    {
        _draftProfileId = _workspace.ConnectedProfileId;
        _draftProfileName = _connectedProfile?.Name;
        OnPropertyChanged(nameof(HasDraftEnvironmentMismatch));
        OnPropertyChanged(nameof(DraftEnvironmentWarning));
        SendDraftCommand.NotifyCanExecuteChanged();
    }

    private ProfileItemViewModel GetConnectedProfileItem()
    {
        var connectedProfileId = _workspace.ConnectedProfileId
            ?? throw new InvalidOperationException("Connect to an environment first.");
        return Profiles.FirstOrDefault(profile => profile.Id == connectedProfileId)
            ?? throw new InvalidOperationException("The connected environment no longer exists.");
    }

    private void NavigateTo(NavigationPage page)
    {
        SelectedNavigation = Navigation.First(item => item.Key == page.ToString());
    }

    private void NotifyPageVisibility()
    {
        OnPropertyChanged(nameof(IsOverviewVisible));
        OnPropertyChanged(nameof(IsExplorerVisible));
        OnPropertyChanged(nameof(IsDeadLettersVisible));
        OnPropertyChanged(nameof(IsComposerVisible));
        OnPropertyChanged(nameof(IsMonitorsVisible));
        OnPropertyChanged(nameof(IsEnvironmentsVisible));
        OnPropertyChanged(nameof(IsActivityVisible));
    }

    private void NotifyConnectionState()
    {
        UpdateProfileConnectionStates();
        OnPropertyChanged(nameof(IsConnected));
        OnPropertyChanged(nameof(ConnectedProfileId));
        OnPropertyChanged(nameof(ConnectedProfileName));
        OnPropertyChanged(nameof(ConnectionLabel));
        OnPropertyChanged(nameof(ConnectionColor));
        OnPropertyChanged(nameof(ConnectedNamespace));
        OnPropertyChanged(nameof(IsSelectedProfileConnected));
        OnPropertyChanged(nameof(CanWrite));
        OnPropertyChanged(nameof(CanUnlockWrites));
        OnPropertyChanged(nameof(HasDraftEnvironmentMismatch));
        OnPropertyChanged(nameof(DraftEnvironmentWarning));
        OnPropertyChanged(nameof(MonitorTargetPreview));
        OnPropertyChanged(nameof(WriteAccessLabel));
        OnPropertyChanged(nameof(WriteAccessColor));
        NotifyCommandStates();
    }

    private void UpdateProfileConnectionStates()
    {
        var connectedProfileId = IsConnected ? _workspace.ConnectedProfileId : null;
        foreach (var profile in Profiles)
        {
            profile.UpdateConnectionState(profile.Id == connectedProfileId);
        }
    }

    private void NotifyStatistics()
    {
        OnPropertyChanged(nameof(QueueCount));
        OnPropertyChanged(nameof(TopicCount));
        OnPropertyChanged(nameof(SubscriptionCount));
        OnPropertyChanged(nameof(ActiveMessageCount));
        OnPropertyChanged(nameof(DeadLetterCount));
        OnPropertyChanged(nameof(GlobalDlqSourceCount));
        OnPropertyChanged(nameof(VisibleDlqSourceCount));
        OnPropertyChanged(nameof(VisibleDlqSourceRowCount));
        OnPropertyChanged(nameof(LastUpdatedText));
    }

    private void NotifyCommandStates()
    {
        EditEnvironmentCommand.NotifyCanExecuteChanged();
        DeleteEnvironmentCommand.NotifyCanExecuteChanged();
        ConnectCommand.NotifyCanExecuteChanged();
        RefreshTopologyCommand.NotifyCanExecuteChanged();
        ScanCurrentEnvironmentCommand.NotifyCanExecuteChanged();
        ScanAllEnvironmentsCommand.NotifyCanExecuteChanged();
        BrowseSelectedActiveCommand.NotifyCanExecuteChanged();
        BrowseSelectedDeadLettersCommand.NotifyCanExecuteChanged();
        BrowseSelectedTransferDeadLettersCommand.NotifyCanExecuteChanged();
        BrowseDlqSourceCommand.NotifyCanExecuteChanged();
        NewMessageCommand.NotifyCanExecuteChanged();
        OpenMessageAsDraftCommand.NotifyCanExecuteChanged();
        SendDraftCommand.NotifyCanExecuteChanged();
        ToggleMonitorCommand.NotifyCanExecuteChanged();
        UnlockWritesCommand.NotifyCanExecuteChanged();
    }

    private void AddActivity(string level, string action, string details)
    {
        Activity.Insert(0, new ActivityItemViewModel(DateTimeOffset.UtcNow, level, action, details));
        while (Activity.Count > 500)
        {
            Activity.RemoveAt(Activity.Count - 1);
        }
    }

    private static string SanitizeException(Exception exception)
    {
        var raw = exception.GetBaseException().Message;
        if (raw.Length > 5_000)
        {
            raw = raw[..5_000];
        }
        var text = SensitiveValuePattern
            .Replace(raw, "$1=[REDACTED]")
            .Replace('\r', ' ')
            .Replace('\n', ' ')
            .Trim();
        return text.Length > 600 ? text[..600] + "…" : text;
    }

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

        var commands = new[]
        {
            AddEnvironmentCommand,
            EditEnvironmentCommand,
            DeleteEnvironmentCommand,
            ConnectCommand,
            RefreshTopologyCommand,
            ScanCurrentEnvironmentCommand,
            ScanAllEnvironmentsCommand,
            BrowseSelectedActiveCommand,
            BrowseSelectedDeadLettersCommand,
            BrowseSelectedTransferDeadLettersCommand,
            BrowseDlqSourceCommand,
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
        catch
        {
            // Shutdown continues after all tasks have reached a terminal state.
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
