using QueueLoom.App.Models;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>Dead-letter sources and peeking. Search, scans and purges live in their own files.</summary>
public sealed partial class MainWindowViewModel
{
    public DeadLetterEnvironmentFilterItemViewModel? SelectedDeadLetterEnvironmentFilter
    {
        get => _selectedDeadLetterEnvironmentFilter;
        set
        {
            var previousProfileId = _selectedDeadLetterEnvironmentFilter?.ProfileId;
            if (SetProperty(ref _selectedDeadLetterEnvironmentFilter, value))
            {
                // The filters are rebuilt whenever the environment list changes; the listed messages and their ticks
                // are only dropped when the scope really moves to another environment.
                if (value?.ProfileId != previousProfileId)
                {
                    Messages.Clear();
                    SelectedMessage = null;
                    ResetBrowsePaging();
                }
                ApplyDeadLetterEnvironmentFilter();
                OnPropertyChanged(nameof(CanPurgeEnvironmentDeadLetters));
                NotifyCommandStates();
            }
        }
    }

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
                OnPropertyChanged(nameof(CanPurgeTopicDeadLetters));
                OnPropertyChanged(nameof(CanPurgeSelectedDeadLetters));
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
                OnPropertyChanged(nameof(CanOpenSelectedMessageAsDraft));
                OnPropertyChanged(nameof(SelectedMessageNeedsReadOnlyPreview));
                NotifyCommandStates();
            }
        }
    }

    public bool HasSelectedMessage => SelectedMessage is not null;

    public bool SelectedMessageNeedsReadOnlyPreview => SelectedMessage is { CanOpenAsDraft: false };

    public bool CanOpenSelectedMessageAsDraft =>
        IsConnected &&
        SelectedMessage is { CanOpenAsDraft: true } message &&
        (message.ProfileId is null || message.ProfileId == ConnectedProfileId);

    public bool CanPurgeEnvironmentDeadLetters =>
        CanWrite &&
        SelectedDeadLetterEnvironmentFilter?.ProfileId is { } profileId &&
        profileId == ConnectedProfileId &&
        _topology is not null;

    public bool CanPurgeTopicDeadLetters =>
        CanWrite &&
        SelectedDlqSource is { IsSubscription: true } source &&
        source.ProfileId == ConnectedProfileId &&
        _topology is not null;

    public bool CanPurgeSelectedDeadLetters =>
        CanWrite &&
        SelectedDlqSource is { } source &&
        source.ProfileId == ConnectedProfileId;

    public string DeadLetterSearchQuery
    {
        get => _deadLetterSearchQuery;
        set
        {
            if (SetProperty(ref _deadLetterSearchQuery, value))
            {
                SearchDeadLettersCommand.NotifyCanExecuteChanged();
                ClearDeadLetterSearchCommand.NotifyCanExecuteChanged();
                SaveSearchCommand?.NotifyCanExecuteChanged();
            }
        }
    }

    public string DeadLetterSearchStatus
    {
        get => _deadLetterSearchStatus;
        private set => SetProperty(ref _deadLetterSearchStatus, value);
    }

    public string MessageListTitle
    {
        get => _messageListTitle;
        private set => SetProperty(ref _messageListTitle, value);
    }

    public long VisibleDlqSourceCount => FilteredDeadLetterSources.Sum(source => source.Count);

    public int VisibleDlqSourceRowCount => FilteredDeadLetterSources.Count;

    private void RefreshDeadLetterEnvironmentFilters()
    {
        // Keep the scope chosen on the Messages page; the Environments list selection only fills a scope that is gone.
        var selectedProfileId = _selectedDeadLetterEnvironmentFilter is { } current && Profiles.Any(profile => profile.Id == current.ProfileId)
            ? current.ProfileId
            : SelectedProfile?.Id;

        DeadLetterEnvironmentFilters.Clear();
        foreach (var profile in Profiles)
        {
            DeadLetterEnvironmentFilters.Add(new DeadLetterEnvironmentFilterItemViewModel(
                profile.Id,
                profile.Name,
                profile.EnvironmentLabel,
                profile.EnvironmentTone));
        }

        SelectedDeadLetterEnvironmentFilter = DeadLetterEnvironmentFilters
            .FirstOrDefault(filter => filter.ProfileId == selectedProfileId)
            ?? DeadLetterEnvironmentFilters.FirstOrDefault();
    }

    private void ApplyDeadLetterEnvironmentFilter()
    {
        var filter = SelectedDeadLetterEnvironmentFilter;
        FilteredDeadLetterSources.Clear();
        foreach (var source in DeadLetterSources.Where(source => filter?.Matches(source) == true))
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
        var resultsGeneration = _messageResultsGeneration;
        if (_workspace.ConnectedProfileId != profile.Id)
        {
            await ConnectProfileAsync(profile, profile.Profile, loadTopology: true, cancellationToken)
                .ConfigureAwait(true);
        }

        if (resultsGeneration != _messageResultsGeneration)
        {
            return;
        }

        ResetBrowsePaging();
        BeginLogRead(profile);
        _browseProfile = profile;
        _browseSource = source;
        _browseSubQueue = subQueue;
        Messages.Clear();
        SelectedMessage = null;
        await LoadBrowsePageAsync(cancellationToken).ConfigureAwait(true);
        if (resultsGeneration != _messageResultsGeneration)
        {
            return;
        }
        MessageListTitle = $"{profile.Name} · {source.DisplayName} · {FormatSubQueue(subQueue)}";
        NavigateTo(NavigationPage.DeadLetters);
        StatusText = profile.Provider == MessagingProvider.AzureServiceBus
            ? $"Peeked {Messages.Count:N0} messages without acquiring locks"
            : $"Read {Messages.Count:N0} messages and released them unchanged";
        AddActivity(
            "Info",
            "Peek",
            $"{source.DisplayName} · {FormatSubQueue(subQueue)} · {Messages.Count:N0} messages",
            source);
    }
}
