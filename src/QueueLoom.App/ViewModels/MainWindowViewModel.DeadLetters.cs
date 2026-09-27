using QueueLoom.App.Models;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>Dead-letter sources, search, peeking and backup-then-purge.</summary>
public sealed partial class MainWindowViewModel
{
    public DeadLetterEnvironmentFilterItemViewModel? SelectedDeadLetterEnvironmentFilter
    {
        get => _selectedDeadLetterEnvironmentFilter;
        set
        {
            if (SetProperty(ref _selectedDeadLetterEnvironmentFilter, value))
            {
                Messages.Clear();
                SelectedMessage = null;
                ResetBrowsePaging();
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
                NotifyCommandStates();
            }
        }
    }

    public bool HasSelectedMessage => SelectedMessage is not null;

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
        var selectedProfileId = SelectedProfile?.Id;

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

    private async Task SearchDeadLettersAsync(CancellationToken cancellationToken)
    {
        ResetBrowsePaging();
        var query = DeadLetterSearchQuery.Trim();
        if (query.Length == 0)
        {
            throw new InvalidOperationException("Enter a search value first.");
        }

        var filter = SelectedDeadLetterEnvironmentFilter
            ?? throw new InvalidOperationException("Select an environment filter first.");
        var profilesToSearch = filter.ProfileId is { } profileId
            ? Profiles.Where(profile => profile.Id == profileId).ToArray()
            : [];
        if (profilesToSearch.Length == 0)
        {
            throw new InvalidOperationException("The selected search scope contains no environments.");
        }

        const int maximumResults = DeadLetterSearchRequest.DefaultMaximumResults;
        var connectedProfileBeforeSearch = _connectedProfile;
        var wasConnected = IsConnected && connectedProfileBeforeSearch is not null;
        var temporaryWriteExpiryBeforeSearch = connectedProfileBeforeSearch is not null &&
                                               _writeUnlockProfileId == connectedProfileBeforeSearch.Id
            ? _writeUnlockExpiresAt
            : null;
        var results = new List<MessageItemViewModel>();
        var scannedMessages = 0;
        var searchedTargets = 0;
        var sourceFailures = 0;
        var environmentFailures = 0;
        var timedOutEnvironments = 0;
        var incomplete = false;
        var restoreFailed = false;
        using var totalSearchTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        totalSearchTimeout.CancelAfter(TimeSpan.FromSeconds(60));
        var searchToken = totalSearchTimeout.Token;

        try
        {
            foreach (var profile in profilesToSearch)
            {
                if (searchToken.IsCancellationRequested)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    timedOutEnvironments++;
                    incomplete = true;
                    break;
                }
                if (results.Count >= maximumResults)
                {
                    incomplete = true;
                    break;
                }

                StatusText = $"Reading topology and DLQ counts in {profile.Name}...";
                try
                {
                    if (_workspace.ConnectedProfileId != profile.Id)
                    {
                        var readOnlyProfile = profile.Profile with { AccessMode = ProfileAccessMode.ReadOnly };
                        await ConnectProfileAsync(profile, readOnlyProfile, loadTopology: false, searchToken)
                            .ConfigureAwait(true);
                    }

                    var topology = await _workspace.GetTopologyAsync(forceRefresh: true, searchToken)
                        .ConfigureAwait(true);
                    var targets = DeadLetterSearchTargets.ForTopology(topology);
                    if (targets.Length == 0)
                    {
                        continue;
                    }

                    StatusText = $"Searching {targets.Length:N0} DLQ sources in {profile.Name}...";
                    using var environmentTimeout = CancellationTokenSource.CreateLinkedTokenSource(searchToken);
                    environmentTimeout.CancelAfter(TimeSpan.FromSeconds(30));
                    try
                    {
                        var search = await _workspace.SearchDeadLettersAsync(
                                new DeadLetterSearchRequest(
                                    query,
                                    targets,
                                    maximumResults: maximumResults - results.Count),
                                environmentTimeout.Token)
                            .ConfigureAwait(true);
                        scannedMessages = checked(scannedMessages + search.ScannedMessageCount);
                        searchedTargets += search.Sources.Count;
                        sourceFailures += search.Sources.Count(source => !source.IsSuccessful);
                        incomplete |= !search.IsComplete;
                        results.AddRange(search.Matches.Select(message => new MessageItemViewModel(
                            message,
                            profile.Id,
                            profile.Name,
                            profile.EnvironmentLabel,
                            profile.EnvironmentTone)));
                        cancellationToken.ThrowIfCancellationRequested();
                        if (environmentTimeout.IsCancellationRequested)
                        {
                            timedOutEnvironments++;
                            incomplete = true;
                            if (totalSearchTimeout.IsCancellationRequested)
                            {
                                break;
                            }
                        }
                    }
                    catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                    {
                        timedOutEnvironments++;
                        incomplete = true;
                        if (totalSearchTimeout.IsCancellationRequested)
                        {
                            break;
                        }
                    }
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    timedOutEnvironments++;
                    incomplete = true;
                    break;
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    environmentFailures++;
                    incomplete = true;
                    AddActivity(
                        "Error",
                        "DLQ search environment failed",
                        $"{profile.Name} | {SanitizeException(exception)}");
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            Messages.Clear();
            SelectedMessage = null;
            MessageListTitle = "Search cancelled";
            DeadLetterSearchStatus = "Search cancelled; partial results were not applied.";
            throw;
        }
        finally
        {
            if (!_isDisposed)
            {
                using var restoreCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                try
                {
                    if (wasConnected && connectedProfileBeforeSearch is not null)
                    {
                        var originalProfile = Profiles.FirstOrDefault(profile =>
                            profile.Id == connectedProfileBeforeSearch.Id);
                        if (originalProfile is not null &&
                            (_workspace.ConnectedProfileId != originalProfile.Id ||
                             _connectedProfile != connectedProfileBeforeSearch))
                        {
                            await RestoreConnectedProfileAsync(
                                    originalProfile,
                                    connectedProfileBeforeSearch,
                                    temporaryWriteExpiryBeforeSearch,
                                    restoreCancellation.Token)
                                .ConfigureAwait(true);
                        }
                    }
                    else if (_workspace.ConnectionState == WorkspaceConnectionState.Connected)
                    {
                        await _workspace.DisconnectAsync(restoreCancellation.Token).ConfigureAwait(true);
                        ClearConnectedState();
                    }
                }
                catch (Exception exception)
                {
                    restoreFailed = true;
                    ClearConnectedState();
                    AddActivity("Error", "Environment restore failed", SanitizeException(exception));
                }
            }
        }

        Messages.Clear();
        foreach (var result in results
                     .OrderBy(result => result.Message.EnqueuedAt ?? DateTimeOffset.MaxValue)
                     .ThenBy(result => result.Message.SequenceNumber)
                     .ThenBy(result => result.ProfileName, StringComparer.OrdinalIgnoreCase))
        {
            Messages.Add(result);
        }
        SelectedMessage = Messages.FirstOrDefault();
        var scopeName = filter.ProfileId.HasValue ? filter.Name : "all environments";
        var qualifier = incomplete || restoreFailed
            ? " | incomplete: limits or errors occurred"
            : " | complete within the current DLQ snapshot";
        DeadLetterSearchStatus =
            $"{Messages.Count:N0} matches | {scannedMessages:N0} messages inspected | " +
            $"{searchedTargets:N0} sources | oldest first{qualifier}" +
            (timedOutEnvironments > 0 ? $" | {timedOutEnvironments:N0} environment timeouts" : string.Empty);
        MessageListTitle = $"Search timeline | {scopeName} | body search reads up to the first 1 MiB";
        StatusText = $"Found {Messages.Count:N0} matching dead-letter messages in {scopeName}";
        AddActivity(
            incomplete || restoreFailed ? "Warning" : "Info",
            "DLQ search",
            $"{scopeName} | {Messages.Count:N0} matches | {scannedMessages:N0} inspected | " +
            $"{sourceFailures:N0} source errors | {environmentFailures:N0} environment errors | " +
            $"{timedOutEnvironments:N0} timeouts");
        ClearDeadLetterSearchCommand.NotifyCanExecuteChanged();
    }

    private void ClearDeadLetterSearch()
    {
        ResetBrowsePaging();
        DeadLetterSearchQuery = string.Empty;
        Messages.Clear();
        SelectedMessage = null;
        MessageListTitle = "Peeked messages";
        DeadLetterSearchStatus = "Search Correlation ID, Message ID, body, or application properties.";
        ClearDeadLetterSearchCommand.NotifyCanExecuteChanged();
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
        var wasConnected = IsConnected && connectedProfileBeforeScan is not null;
        var temporaryWriteExpiryBeforeScan = connectedProfileBeforeScan is not null &&
                                             _writeUnlockProfileId == connectedProfileBeforeScan.Id
            ? _writeUnlockExpiresAt
            : null;
        _lastDlqMeasurements.Clear();
        DeadLetterSources.Clear();
        ApplyDeadLetterEnvironmentFilter();
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
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _lastDlqScanHadFailures = true;
            _lastDlqMeasurements.Clear();
            DeadLetterSources.Clear();
            ApplyDeadLetterEnvironmentFilter();
            NotifyStatistics();
            throw;
        }
        finally
        {
            if (!_isDisposed)
            {
                if (wasConnected && connectedProfileBeforeScan is not null)
                {
                    var preferred = Profiles.FirstOrDefault(profile => profile.Id == connectedProfileBeforeScan.Id);
                    using var restoreCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                    try
                    {
                        if (preferred is null)
                        {
                            throw new InvalidOperationException("The previously connected environment no longer exists.");
                        }
                        await RestoreConnectedProfileAsync(
                                preferred,
                                connectedProfileBeforeScan,
                                temporaryWriteExpiryBeforeScan,
                                restoreCancellation.Token)
                            .ConfigureAwait(true);
                    }
                    catch (Exception exception)
                    {
                        restoreFailed = true;
                        _lastDlqScanHadFailures = true;
                        ClearConnectedState();
                        AddActivity(
                            "Error",
                            "Environment restore failed",
                            $"{preferred?.Name ?? connectedProfileBeforeScan.Name} · {SanitizeException(exception)}");
                    }
                }
                else
                {
                    using var restoreCancellation = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                    try
                    {
                        if (_workspace.ConnectionState == WorkspaceConnectionState.Connected)
                        {
                            await _workspace.DisconnectAsync(restoreCancellation.Token).ConfigureAwait(true);
                        }
                        ClearConnectedState();
                    }
                    catch (Exception exception)
                    {
                        restoreFailed = true;
                        _lastDlqScanHadFailures = true;
                        ClearConnectedState();
                        AddActivity("Error", "Offline state restore failed", SanitizeException(exception));
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
        _hasDlqScan = true;
        OnPropertyChanged(nameof(GlobalDlqDisplay));
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
                profile.EnvironmentTone,
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

    private async Task BrowseSelectedDlqSourceAsync(CancellationToken cancellationToken)
    {
        var source = SelectedDlqSource ?? throw new InvalidOperationException("Select a DLQ source first.");
        var profile = Profiles.FirstOrDefault(item => item.Id == source.ProfileId)
            ?? throw new InvalidOperationException("The source environment no longer exists.");
        await BrowseAsync(profile, source.Entity, source.Snapshot.SubQueue, cancellationToken)
            .ConfigureAwait(true);
    }

    private Task PurgeEnvironmentDeadLettersAsync(CancellationToken cancellationToken)
    {
        var filter = SelectedDeadLetterEnvironmentFilter
            ?? throw new InvalidOperationException("Select an environment filter first.");
        var profile = GetConnectedProfileItem();
        if (filter.ProfileId != profile.Id)
        {
            throw new InvalidOperationException(
                "Select the connected environment in the dead-letter filter before purging it.");
        }

        if (_topology is null)
        {
            throw new InvalidOperationException("Refresh the connected environment topology first.");
        }
        return BackupAndPurgeDeadLettersAsync(
            $"environment '{profile.Name}'",
            GetKnownPurgeTargets(profile.Id, _ => true),
            cancellationToken);
    }

    private Task PurgeTopicDeadLettersAsync(CancellationToken cancellationToken)
    {
        var selection = SelectedDlqSource
            ?? throw new InvalidOperationException("Select a subscription source first.");
        if (!selection.IsSubscription || string.IsNullOrWhiteSpace(selection.ParentTopicName))
        {
            throw new InvalidOperationException("Select a subscription to purge every subscription under its topic.");
        }
        EnsurePurgeSelectionUsesConnectedEnvironment(selection);

        if (_topology?.Topics.Any(item =>
                string.Equals(item.Name, selection.ParentTopicName, StringComparison.Ordinal)) != true)
        {
            throw new InvalidOperationException("The selected topic is no longer present in the connected topology.");
        }

        return BackupAndPurgeDeadLettersAsync(
            $"all non-empty subscriptions under topic '{selection.ParentTopicName}'",
            GetKnownPurgeTargets(
                selection.ProfileId,
                source => source.Kind == ServiceBusEntityKind.Subscription &&
                          string.Equals(source.TopicName, selection.ParentTopicName, StringComparison.Ordinal)),
            cancellationToken);
    }

    private Task PurgeSelectedDeadLettersAsync(CancellationToken cancellationToken)
    {
        var selection = SelectedDlqSource
            ?? throw new InvalidOperationException("Select a queue or subscription first.");
        EnsurePurgeSelectionUsesConnectedEnvironment(selection);
        var targetKind = selection.IsSubscription ? "subscription" : "queue";
        return BackupAndPurgeDeadLettersAsync(
            $"{targetKind} '{selection.EntityPath}'",
            GetKnownPurgeTargets(selection.ProfileId, source => source == selection.Entity),
            cancellationToken);
    }

    private IReadOnlyList<DeadLetterPurgeTarget> GetKnownPurgeTargets(
        Guid profileId,
        Func<ServiceBusEntityReference, bool> includesSource)
    {
        return DeadLetterSources
            .Where(row => row.ProfileId == profileId &&
                          row.Snapshot.IsSuccessful &&
                          row.Count > 0 &&
                          includesSource(row.Entity))
            .OrderBy(row => row.Entity.TopicName ?? row.Entity.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(row => row.Entity.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(row => row.Snapshot.SubQueue)
            .Select(row => new DeadLetterPurgeTarget(row.Entity, row.Snapshot.SubQueue))
            .Distinct()
            .ToArray();
    }

    private void EnsurePurgeSelectionUsesConnectedEnvironment(DlqSourceItemViewModel selection)
    {
        if (selection.ProfileId != ConnectedProfileId)
        {
            throw new InvalidOperationException(
                "The selected source belongs to another environment. Connect to that environment before purging.");
        }
    }

    private async Task BackupAndPurgeDeadLettersAsync(
        string targetDescription,
        IReadOnlyList<DeadLetterPurgeTarget> targets,
        CancellationToken cancellationToken)
    {
        if (targets.Count == 0)
        {
            throw new InvalidOperationException(
                "The latest scan contains no non-empty dead-letter sources in this scope. Scan the environment again first.");
        }
        if (!CanWrite)
        {
            throw new InvalidOperationException("Unlock write access before purging dead letters.");
        }

        var connectedProfileId = ConnectedProfileId
            ?? throw new InvalidOperationException("Connect to an environment first.");
        var targetKeys = targets.Select(target => (target.Source, target.SubQueue)).ToHashSet();
        var knownCount = DeadLetterSources
            .Where(item => item.ProfileId == connectedProfileId &&
                           targetKeys.Contains((item.Entity, item.Snapshot.SubQueue)))
            .Sum(item => item.Count);
        if (!HasValidPurgeLimit)
            throw new InvalidOperationException("Choose a purge limit between 1 and 10,000 per source.");
        var limit = (int)PurgeLimitPerSource!.Value;
        var profile = _connectedProfile ?? throw new InvalidOperationException("Connect first.");
        var scope = string.Join("\n", targets.Take(20).Select(t => $"• {t.Source.Path} / {t.SubQueue}"));
        if (targets.Count > 20) scope += $"\n… and {targets.Count - 20} additional sources";
        var confirmed = await _dialogs.ConfirmAsync("Review backup and purge",
            $"Environment: {profile.Name}\n{profile.Provider.DisplayName()}: {profile.EndpointDisplay}\n" +
            $"Sources: {targets.Count}\nKnown messages: {knownCount:N0} (latest scan; may be stale)\n" +
            $"Hard limit: {limit:N0} per source\nBackup folder: {BackupRootDirectory}\n\n{scope}\n\n" +
            "This receives and permanently deletes messages after backup. New arrivals can be included up to the limit. " +
            "Cancellation stops future work; completed deletions are not undone.",
            isDangerous: true, requiredText: profile.Environment == EnvironmentKind.Production ? profile.Name : null,
            cancellationToken: cancellationToken).ConfigureAwait(true);
        if (!confirmed) { StatusText = "Purge cancelled before any messages changed"; return; }
        if (!CanWrite || ConnectedProfileId != connectedProfileId)
            throw new InvalidOperationException("Write access or environment changed. Review the purge again.");
        RecordOperationIntent("Purge started", $"{targetDescription} · {targets.Count} sources · limit {limit} per source", null);
        StatusText =
            $"Backing up and purging {knownCount:N0} known messages from {targetDescription}...";

        using var purgeCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var temporaryUnlockExpiresAt = _writeUnlockProfileId == connectedProfileId
            ? _writeUnlockExpiresAt
            : null;
        if (temporaryUnlockExpiresAt is { } expiresAt)
        {
            var remaining = expiresAt - DateTimeOffset.UtcNow;
            if (remaining <= TimeSpan.Zero)
            {
                throw new InvalidOperationException("Temporary write access expired before the purge started.");
            }
            purgeCancellation.CancelAfter(remaining);
        }

        DeadLetterPurgeResult result;
        var progress = new Progress<DeadLetterPurgeProgress>(update =>
        {
            var subQueue = update.SubQueue == ServiceBusSubQueue.TransferDeadLetter
                ? "transfer DLQ"
                : "DLQ";
            StatusText = update.Stage switch
            {
                DeadLetterPurgeStage.Starting =>
                    $"Source {update.TargetNumber}/{update.TargetCount} · opening {update.Source.DisplayName} {subQueue}",
                DeadLetterPurgeStage.BackingUp =>
                    $"Source {update.TargetNumber}/{update.TargetCount} · backing up {update.Source.DisplayName} · {update.BackedUpCount:N0} saved",
                DeadLetterPurgeStage.Deleting =>
                    $"Source {update.TargetNumber}/{update.TargetCount} · backup complete · deleting {update.Source.DisplayName}",
                DeadLetterPurgeStage.Verifying =>
                    $"Source {update.TargetNumber}/{update.TargetCount} · verifying {update.Source.DisplayName} is empty",
                DeadLetterPurgeStage.Completed =>
                    $"Source {update.TargetNumber}/{update.TargetCount} complete · {update.DeletedCount:N0} deleted",
                _ => StatusText
            };
        });
        try
        {
            result = await _workspace.PurgeDeadLettersAsync(
                    new DeadLetterPurgeRequest(targets, batchSize: 20, maximumMessagesPerSubQueue: limit),
                    purgeCancellation.Token,
                    progress)
                .ConfigureAwait(true);
        }
        catch (OperationCanceledException) when (
            !cancellationToken.IsCancellationRequested &&
            temporaryUnlockExpiresAt.HasValue &&
            DateTimeOffset.UtcNow >= temporaryUnlockExpiresAt.Value)
        {
            throw new InvalidOperationException(
                "Temporary write access expired during the purge. Some messages may already have been deleted; rescan the environment.");
        }

        Messages.Clear();
        SelectedMessage = null;
        MessageListTitle = "Select a queue or subscription, then Peek.";
        ApplyCompletedPurgeToDeadLetterRows(result);
        MessageListTitle = $"Backup saved to {result.BackupDirectory}";
        _backupsLoaded = false;
        if (CurrentPage == NavigationPage.Backups && _backupRepository is not null)
        {
            _pendingBackupRefresh = true;
        }

        var failures = result.Sources.Count(source => !source.IsSuccessful);
        var pendingVerifications = result.Sources.Count(source => source.VerificationPending);
        StatusText = failures > 0
            ? $"Partial backup/purge · {result.DeletedCount:N0} deleted · {failures:N0} source errors"
            : pendingVerifications > 0
                ? $"Backed up and purged {result.DeletedCount:N0} messages · Azure counters are refreshing; rescan recommended"
                : $"Backed up and purged {result.DeletedCount:N0} dead-letter messages from {targetDescription}";
        AddActivity(
            failures == 0 ? "Warning" : "Error",
            failures == 0 ? "Dead letters backed up and purged" : "Partial dead-letter backup/purge",
            $"{targetDescription} · {result.DeletedCount:N0} backed up and deleted · " +
            $"{failures:N0} errors · {pendingVerifications:N0} counters pending · {result.BackupDirectory}");
        if (failures > 0)
        {
            var firstError = result.Sources.First(source => !source.IsSuccessful).Error;
            var sanitizedError = string.IsNullOrWhiteSpace(firstError)
                ? "The safety limit was reached."
                : SanitizeException(new InvalidOperationException(firstError));
            ErrorText =
                $"Some dead-letter sources could not be fully backed up and purged. {sanitizedError} " +
                $"Backup folder: {result.BackupDirectory}";
        }
    }

    private void ApplyCompletedPurgeToDeadLetterRows(DeadLetterPurgeResult result)
    {
        var completedSources = result.Sources
            .Where(source => source.IsSuccessful)
            .Select(source => (source.Source, source.SubQueue))
            .ToHashSet();
        foreach (var row in DeadLetterSources
                     .Where(row => completedSources.Contains((row.Entity, row.Snapshot.SubQueue)))
                     .ToArray())
        {
            DeadLetterSources.Remove(row);
            _previousDlqCounts[$"{row.ProfileId:N}|{row.Entity.Path}|{row.Snapshot.SubQueue}"] = 0;
        }

        SortDeadLetterSources(null, null, null);
        NotifyStatistics();
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

        ResetBrowsePaging();
        _browseProfile = profile;
        _browseSource = source;
        _browseSubQueue = subQueue;
        Messages.Clear();
        SelectedMessage = null;
        await LoadBrowsePageAsync(cancellationToken).ConfigureAwait(true);
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
