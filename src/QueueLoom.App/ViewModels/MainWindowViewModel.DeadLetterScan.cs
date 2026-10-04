using QueueLoom.App.Models;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>Scanning environments for dead letters and keeping the source list up to date.</summary>
public sealed partial class MainWindowViewModel
{
    private async Task ScanCurrentEnvironmentAsync(CancellationToken cancellationToken)
    {
        var profile = GetConnectedProfileItem();

        _lastDlqMeasurements.Clear();
        var snapshot = await _workspace.GetDeadLetterSnapshotAsync(DeadLetterMonitorScope.All, cancellationToken)
            .ConfigureAwait(true);
        CaptureDlqMeasurements(profile.Id, snapshot);
        RecordDeadLetterHistory(profile, snapshot);
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
        // Connecting each environment in turn clears the listed messages and the draft destination of the one it
        // leaves. The sweep ends where it started, so they are put back: re-reading them would cost another delivery
        // on SQS, Pub/Sub and RabbitMQ, and the operator's ticks would be gone.
        var messagesBeforeScan = Messages.ToArray();
        var selectedMessageBeforeScan = SelectedMessage;
        var destinationBeforeScan = SelectedDestination?.Reference;
        var resultsGenerationBeforeScan = _messageResultsGeneration;
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
                    RecordDeadLetterHistory(profile, snapshot);
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
                        if (_connectedProfile?.Id == connectedProfileBeforeScan.Id)
                        {
                            if (resultsGenerationBeforeScan == _messageResultsGeneration && Messages.Count == 0 &&
                                messagesBeforeScan.Length > 0)
                            {
                                using (BatchMessageUpdates())
                                {
                                    foreach (var message in messagesBeforeScan)
                                    {
                                        Messages.Add(message);
                                    }
                                }
                                SelectedMessage = selectedMessageBeforeScan;
                            }
                            if (destinationBeforeScan is not null && SelectedDestination is null)
                            {
                                SelectedDestination = Destinations.FirstOrDefault(item => item.Reference == destinationBeforeScan);
                            }
                        }
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
                withHistory,
                profile.Provider == Core.Profiles.MessagingProvider.Kafka ? "topic" : "queue"));
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
}
