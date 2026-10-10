using QueueLoom.App.Models;
using QueueLoom.App.Services;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>The in-app dead-letter monitor.</summary>
public sealed partial class MainWindowViewModel
{
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

    public bool HasMonitorNotifications => MonitorNotifications.Count > 0;

    public int MonitorNotificationCount => MonitorNotifications.Count;

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

    private async Task StopMonitorForConfigurationChangeAsync(Guid changedProfileId)
    {
        // A monitor pinned to another environment is unaffected; stopping it would silently end its alerts.
        if (!IsMonitoring ||
            _activeMonitorScope != AllEnvironmentsMonitorScope && _monitoredProfileId != changedProfileId)
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
                var checkInterrupted = false;
                string? checkError = null;
                try
                {
                    if (!IsBusy &&
                        await _workspaceGate.WaitAsync(0, cancellationToken).ConfigureAwait(true))
                    {
                        using var checkCancellation =
                            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                        _monitorCheckCancellation = checkCancellation;
                        try
                        {
                            ranCheck = true;
                            checkComplete = await RunMonitorCheckAsync(checkCancellation.Token).ConfigureAwait(true);
                        }
                        catch (OperationCanceledException) when (
                            checkCancellation.IsCancellationRequested &&
                            !cancellationToken.IsCancellationRequested)
                        {
                            checkInterrupted = true;
                        }
                        finally
                        {
                            if (ReferenceEquals(_monitorCheckCancellation, checkCancellation))
                            {
                                _monitorCheckCancellation = null;
                            }
                            _workspaceGate.Release();
                        }
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    ranCheck = true;
                    checkError = SanitizeException(exception);
                    MonitorAlert = checkError;
                    AddActivity("Error", "Monitor check failed", $"{checkError} · retry scheduled");
                }

                MonitorStatus = checkError is not null
                    ? $"{_activeMonitorTargetLabel} · check failed at {DateTimeOffset.Now:HH:mm:ss} · retry in {_activeMonitorIntervalSeconds}s"
                    : checkInterrupted
                        ? $"{_activeMonitorTargetLabel} · check paused for an interactive operation · retry in {_activeMonitorIntervalSeconds}s"
                    : !ranCheck
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
    }

    private async Task<bool> RunMonitorCheckAsync(CancellationToken cancellationToken)
    {
        _lastDlqMeasurements.Clear();
        var isComplete = true;
        var activeScope = _activeMonitorScope ?? MonitorScope;
        var profilesToCheck = activeScope switch
        {
            AllEnvironmentsMonitorScope => Profiles.ToArray(),
            _ =>
            [Profiles.FirstOrDefault(profile => profile.Id == _monitoredProfileId)
             ?? throw new InvalidOperationException("The monitored environment no longer exists.")]
        };
        var originalConnection = _connectedProfile;
        var wasConnected = IsConnected && originalConnection is not null;
        var workspaceWasReconnected = false;

        try
        {
            foreach (var profile in profilesToCheck)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    if (_workspace.ConnectedProfileId != profile.Id)
                    {
                        await _workspace.ConnectAsync(
                                profile.Profile with { AccessMode = ProfileAccessMode.ReadOnly },
                                cancellationToken)
                            .ConfigureAwait(true);
                        workspaceWasReconnected = true;
                    }

                    var scope = activeScope == SelectedSourceMonitorScope
                        ? DeadLetterMonitorScope.ForEntity(
                            _monitoredEntity ?? throw new InvalidOperationException("The monitored source no longer exists."))
                        : DeadLetterMonitorScope.All;
                    var snapshot = await _workspace.GetDeadLetterSnapshotAsync(scope, cancellationToken)
                        .ConfigureAwait(true);
                    CaptureMonitorSnapshot(profile, snapshot);
                    if (activeScope != SelectedSourceMonitorScope)
                    {
                        await RecordDeadLetterHistoryAsync(profile, snapshot, cancellationToken).ConfigureAwait(true);
                    }
                    isComplete &= !snapshot.HasFailures;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception exception)
                {
                    isComplete = false;
                    AddActivity(
                        "Error",
                        "Monitor environment failed",
                        $"{profile.Name} · {SanitizeException(exception)}");
                }
            }
        }
        finally
        {
            using var restoreTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            if (_isDisposed)
            {
                // Closing: the workspace is disposed next. Reconnecting the original environment would only delay the
                // exit (up to the restore timeout) and could ask for a sign-in, as the global scan and search avoid too.
            }
            else if (wasConnected && originalConnection is not null)
            {
                if (workspaceWasReconnected || _workspace.ConnectedProfileId != originalConnection.Id)
                {
                    try
                    {
                        await _workspace.ConnectAsync(originalConnection, restoreTimeout.Token).ConfigureAwait(true);
                    }
                    catch
                    {
                        ClearConnectedState();
                        throw;
                    }
                }
            }
            else if (_workspace.ConnectionState == WorkspaceConnectionState.Connected)
            {
                await _workspace.DisconnectAsync(restoreTimeout.Token).ConfigureAwait(true);
            }
        }

        var total = _lastDlqMeasurements.Values.Sum(measurement => measurement.Count);
        var totalQuality = DeadLetterCountQualities.Combine(_lastDlqMeasurements.Values.Select(measurement => measurement.Quality));
        if (!isComplete)
        {
            MonitorAlert = $"DLQ check was incomplete at {DateTimeOffset.Now:HH:mm:ss}; known counts were not used as a new baseline.";
            AddActivity("Error", "Partial monitor check", MonitorAlert);
            return false;
        }

        ReconcileMonitorNotifications(profilesToCheck, activeScope);

        var increases = _hasMonitorBaseline
            ? _lastDlqMeasurements
                // Only proven growth: a source new to the baseline counts from zero, and a sampled count is compared
                // only with an exact earlier one (1,000 → 300 → 1,000 samples are not "increased by 700").
                .Select(measurement => DeadLetterMeasurement.ProvenIncrease(
                    _monitorBaseline.TryGetValue(measurement.Key, out var before) ? before : new DeadLetterMeasurement(0, false),
                    measurement.Value))
                .OfType<DeadLetterMeasurement>()
                .Where(increase => increase.Count > 0)
                .ToArray()
            : [];
        if (_hasMonitorBaseline && increases.Length > 0)
        {
            // "at least" growth from a sample stays "N+" in the total too.
            var increase = DeadLetterCountText.Format(increases.Sum(item => item.Count), DeadLetterCountQualities.Combine(increases.Select(item => item.Quality)));
            MonitorAlert = $"{increases.Length:N0} DLQ source(s) increased by {increase}; total is {DeadLetterCountText.Format(total, totalQuality)} at {DateTimeOffset.Now:HH:mm:ss}";
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

    private void ReconcileMonitorNotifications(
        IReadOnlyCollection<ProfileItemViewModel> checkedProfiles,
        string activeScope)
    {
        var prefixes = checkedProfiles
            .Select(profile => $"{profile.Id:N}|")
            .ToArray();
        var removedAny = false;

        foreach (var pair in _monitorNotifications.ToArray())
        {
            if (!prefixes.Any(prefix => pair.Key.StartsWith(prefix, StringComparison.Ordinal)) ||
                activeScope == SelectedSourceMonitorScope && pair.Value.Source != _monitoredEntity ||
                _lastDlqMeasurements.ContainsKey(pair.Key))
            {
                continue;
            }

            _monitorNotifications.Remove(pair.Key);
            MonitorNotifications.Remove(pair.Value);
            removedAny = true;
            AddActivity(
                "Success",
                "DLQ source resolved",
                $"{pair.Value.EnvironmentName} · {pair.Value.SourceName} · no longer reported",
                pair.Value.Source);
        }

        if (removedAny)
        {
            NotifyMonitorNotificationsChanged();
        }
    }

    private void CaptureMonitorSnapshot(ProfileItemViewModel profile, DeadLetterSnapshot snapshot)
    {
        var detectedAt = DateTimeOffset.UtcNow;
        // Everything one check finds goes out as one alert: a broker outage that dead-letters into fifty queues sends
        // one notification and one webhook post, not fifty (most of which the in-flight limit would then drop).
        var alerts = new List<MonitorAlert>();
        var changes = new List<(ServiceBusEntityReference Entity, string Text)>();
        foreach (var entity in snapshot.Entities.Where(item => item.IsSuccessful && item.Count.HasValue))
        {
            var key = $"{profile.Id:N}|{entity.Entity.Path}|{entity.SubQueue}";
            var count = entity.Count!.Value;
            var lowerBound = entity.CountIsLowerBound;
            var quality = entity.CountQuality;
            _lastDlqMeasurements[key] = DeadLetterMeasurement.Of(entity);
            if (count <= 0 && lowerBound)
            {
                // A sample that showed nothing does not prove the queue is empty: an open notification stays open, and
                // its count is no longer shown as exact.
                if (_monitorNotifications.TryGetValue(key, out var unproven)) unproven.CountQuality = DeadLetterCountQuality.LowerBound;
                continue;
            }
            if (count <= 0)
            {
                if (_monitorNotifications.Remove(key, out var resolved))
                {
                    MonitorNotifications.Remove(resolved);
                    AddActivity(
                        "Success",
                        "DLQ resolved",
                        $"{profile.Name} · {entity.Entity.DisplayName} · {FormatSubQueue(entity.SubQueue)} · cleared" +
                        (quality == DeadLetterCountQuality.Estimated ? " (by an approximate count)" : string.Empty),
                        entity.Entity);
                }
                continue;
            }

            if (_monitorNotifications.TryGetValue(key, out var existing))
            {
                var before = new DeadLetterMeasurement(existing.Count, existing.CountQuality);
                var now = new DeadLetterMeasurement(count, quality);
                if (lowerBound && count < existing.Count)
                {
                    // Lost precision is not a decrease: a smaller sample keeps the larger count already reported.
                    existing.CountQuality = DeadLetterCountQuality.LowerBound;
                    continue;
                }
                if (before != now)
                {
                    existing.Count = count;
                    existing.CountQuality = quality;
                    existing.LastDetectedAt = detectedAt;
                    // Alert on proven growth only; a change from or to a sample is reported, not alerted as growth.
                    if (DeadLetterMeasurement.ProvenIncrease(before, now) is { Count: > 0 })
                    {
                        alerts.Add(new MonitorAlert(profile.Name, $"{entity.Entity.DisplayName} ({FormatSubQueue(entity.SubQueue)})", count, before.Count)
                            { CountQuality = quality, PreviousQuality = before.Quality });
                    }
                    changes.Add((entity.Entity, $"{entity.Entity.DisplayName} · {before} → {now}"));
                }
                continue;
            }

            var notification = new MonitorNotificationItemViewModel(
                key,
                profile.Name,
                entity.Entity,
                FormatSubQueue(entity.SubQueue),
                count,
                detectedAt) { CountQuality = quality };
            _monitorNotifications[key] = notification;
            MonitorNotifications.Insert(0, notification);
            alerts.Add(new MonitorAlert(profile.Name, $"{entity.Entity.DisplayName} ({FormatSubQueue(entity.SubQueue)})", count, null)
                { CountQuality = quality });
            AddActivity(
                "Warning",
                "DLQ detected",
                $"{profile.Name} · {entity.Entity.DisplayName} · {FormatSubQueue(entity.SubQueue)} · {DeadLetterCountText.Format(count, quality)} messages",
                entity.Entity);
        }

        ReportCountChanges(profile, changes);
        if (alerts.Count > 0)
        {
            RaiseMonitorAlert(QueueLoom.App.Services.MonitorAlert.Combine(alerts));
        }
        NotifyMonitorNotificationsChanged();
    }

    /// <summary>At most this many count changes of one check are listed one by one; more become one entry.</summary>
    internal const int MaximumSeparateCountChanges = 3;

    /// <summary>
    /// A few changed queues keep one entry each, linked to the queue. When a check finds many (an outage moving dead
    /// letters in dozens of queues, every check), they become one entry: each Activity entry is a file written to disk
    /// on the window's thread, and fifty of them per check would stall it and bury everything else in Activity.
    /// </summary>
    private void ReportCountChanges(ProfileItemViewModel profile, List<(ServiceBusEntityReference Entity, string Text)> changes)
    {
        if (changes.Count <= MaximumSeparateCountChanges)
        {
            foreach (var (entity, text) in changes)
            {
                AddActivity("Warning", "DLQ count changed", $"{profile.Name} · {text}", entity);
            }
            return;
        }
        const int listed = 5;
        AddActivity("Warning", "DLQ counts changed",
            $"{profile.Name} · {changes.Count:N0} queues · " + string.Join("; ", changes.Take(listed).Select(change => change.Text)) +
            (changes.Count > listed ? $"; and {changes.Count - listed:N0} more" : string.Empty));
    }

    private void ClearMonitorNotifications()
    {
        _monitorNotifications.Clear();
        MonitorNotifications.Clear();
        MonitorAlert = string.Empty;
        NotifyMonitorNotificationsChanged();
    }

    private void NotifyMonitorNotificationsChanged()
    {
        OnPropertyChanged(nameof(HasMonitorNotifications));
        OnPropertyChanged(nameof(MonitorNotificationCount));
        Navigation.First(item => item.Key == nameof(NavigationPage.Monitors)).AlertCount = MonitorNotifications.Count;
        ClearMonitorNotificationsCommand.NotifyCanExecuteChanged();
    }
}
