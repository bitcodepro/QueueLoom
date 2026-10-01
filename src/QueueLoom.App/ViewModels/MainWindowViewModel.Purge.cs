using QueueLoom.App.Models;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>Backing up and purging dead-letter queues.</summary>
public sealed partial class MainWindowViewModel
{
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
                     .Where(row => row.ProfileId == result.ProfileId && completedSources.Contains((row.Entity, row.Snapshot.SubQueue)))
                     .ToArray())
        {
            DeadLetterSources.Remove(row);
            _previousDlqCounts[$"{row.ProfileId:N}|{row.Entity.Path}|{row.Snapshot.SubQueue}"] = 0;
        }

        SortDeadLetterSources(null, null, null);
        NotifyStatistics();
    }
}
