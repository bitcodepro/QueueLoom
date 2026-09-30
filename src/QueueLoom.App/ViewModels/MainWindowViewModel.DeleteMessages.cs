using System.Collections.Specialized;
using System.ComponentModel;
using QueueLoom.App.Commands;
using QueueLoom.App.Models;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>Backs up and deletes exactly the messages the operator ticked, e.g. the results of a DLQ search.</summary>
public sealed partial class MainWindowViewModel
{
    public AsyncRelayCommand DeleteMarkedMessagesCommand { get; private set; } = null!;

    public int MarkedMessageCount => Messages.Count(message => message.IsMarked);

    public bool HasMarkedMessages => Messages.Any(message => message.IsMarked);

    public bool HasDeletableMessages => Messages.Any(message => message.CanDelete);

    public string DeleteMarkedMessagesLabel => MarkedMessageCount switch
    {
        0 => "Delete selected…",
        1 => "Delete 1 message…",
        var count => $"Delete {count:N0} messages…"
    };

    /// <summary>Header checkbox: true when every deletable message is ticked, false when none, null otherwise.</summary>
    public bool? AreAllMessagesMarked
    {
        get
        {
            var deletable = Messages.Where(message => message.CanDelete).ToArray();
            var marked = deletable.Count(message => message.IsMarked);
            return marked == 0 ? false : marked == deletable.Length ? true : null;
        }
        set
        {
            var mark = value == true;
            foreach (var message in Messages)
            {
                message.IsMarked = mark;
            }
        }
    }

    private void InitializeMessageDeletion()
    {
        DeleteMarkedMessagesCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Deleting selected messages", DeleteMarkedMessagesAsync, token, allowCancellation: true),
            () => !IsBusy && CanWrite && HasMarkedMessages);
        Messages.CollectionChanged += OnMessagesChangedForDeletion;
    }

    private void OnMessagesChangedForDeletion(object? sender, NotifyCollectionChangedEventArgs args)
    {
        foreach (var item in args.OldItems?.OfType<MessageItemViewModel>() ?? [])
        {
            item.PropertyChanged -= OnMessageMarkChanged;
        }
        foreach (var item in args.NewItems?.OfType<MessageItemViewModel>() ?? [])
        {
            item.PropertyChanged += OnMessageMarkChanged;
        }
        NotifyMarkedMessages();
    }

    private void OnMessageMarkChanged(object? sender, PropertyChangedEventArgs args)
    {
        if (args.PropertyName == nameof(MessageItemViewModel.IsMarked))
        {
            NotifyMarkedMessages();
        }
    }

    private void NotifyMarkedMessages()
    {
        OnPropertyChanged(nameof(MarkedMessageCount));
        OnPropertyChanged(nameof(HasMarkedMessages));
        OnPropertyChanged(nameof(HasDeletableMessages));
        OnPropertyChanged(nameof(DeleteMarkedMessagesLabel));
        OnPropertyChanged(nameof(ResendMarkedMessagesLabel));
        OnPropertyChanged(nameof(AreAllMessagesMarked));
        DeleteMarkedMessagesCommand.NotifyCanExecuteChanged();
        ResendMarkedMessagesCommand?.NotifyCanExecuteChanged();
    }

    private async Task DeleteMarkedMessagesAsync(CancellationToken cancellationToken)
    {
        var marked = Messages.Where(message => message.IsMarked).ToArray();
        if (marked.Length == 0)
        {
            throw new InvalidOperationException("Tick the messages to delete first.");
        }
        if (!CanWrite)
        {
            throw new InvalidOperationException("Unlock write access before deleting messages.");
        }
        if (marked.Any(message => !message.CanDelete))
        {
            throw new InvalidOperationException("Only dead-lettered messages can be deleted. Untick active messages.");
        }
        if (marked.Length > DeleteDeadLetterMessagesRequest.MaximumMessages)
        {
            throw new InvalidOperationException(
                $"At most {DeleteDeadLetterMessagesRequest.MaximumMessages:N0} messages can be deleted at once. Narrow the selection.");
        }

        var connectedProfileId = ConnectedProfileId
            ?? throw new InvalidOperationException("Connect to the messages' environment first.");
        if (marked.Any(message => message.ProfileId is { } profileId && profileId != connectedProfileId))
        {
            throw new InvalidOperationException(
                "Some ticked messages belong to another environment. Connect that environment and search again.");
        }

        var profile = _connectedProfile ?? throw new InvalidOperationException("Connect first.");
        var request = new DeleteDeadLetterMessagesRequest(marked.Select(message => message.Key));
        var sources = request.BySubQueue
            .Select(group => $"• {group.Key.Source.DisplayName} / {FormatSubQueue(group.Key.SubQueue)}: {group.Count():N0}")
            .ToArray();
        var scope = string.Join("\n", sources.Take(15));
        if (sources.Length > 15)
        {
            scope += $"\n… and {sources.Length - 15} more dead-letter queues";
        }

        var confirmed = await _dialogs.ConfirmAsync(
            "Delete selected messages",
            $"Environment: {profile.Name}\n{profile.Provider.DisplayName()}: {profile.EndpointDisplay}\n" +
            $"Messages: {marked.Length:N0} (exactly the ticked ones)\nBackup folder: {BackupRootDirectory}\n\n{scope}\n\n" +
            "Each message is saved to a local backup first and then permanently deleted. " +
            "Other messages in these dead-letter queues are only locked briefly while QueueLoom looks for the selection " +
            "and are released unchanged (their delivery count increases by one). " +
            "Cancellation stops future work; completed deletions are not undone.",
            isDangerous: true,
            requiredText: profile.Environment == EnvironmentKind.Production ? profile.Name : null,
            cancellationToken: cancellationToken).ConfigureAwait(true);
        if (!confirmed)
        {
            StatusText = "Deletion cancelled before any messages changed";
            return;
        }
        if (!CanWrite || ConnectedProfileId != connectedProfileId)
        {
            throw new InvalidOperationException("Write access or environment changed. Review the deletion again.");
        }

        RecordOperationIntent(
            "Delete selected messages started",
            $"{marked.Length:N0} messages in {sources.Length:N0} dead-letter queues",
            null);

        using var deleteCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var temporaryUnlockExpiresAt = _writeUnlockProfileId == connectedProfileId ? _writeUnlockExpiresAt : null;
        if (temporaryUnlockExpiresAt is { } expiresAt)
        {
            var remaining = expiresAt - DateTimeOffset.UtcNow;
            if (remaining <= TimeSpan.Zero)
            {
                throw new InvalidOperationException("Temporary write access expired before the deletion started.");
            }
            deleteCancellation.CancelAfter(remaining);
        }

        var progress = new Progress<DeadLetterMessageDeletionProgress>(update =>
            StatusText = $"Queue {update.QueueNumber}/{update.QueueCount} · {update.Source.DisplayName} · " +
                         $"{update.ScannedCount:N0} checked · {update.DeletedCount:N0} of {marked.Length:N0} deleted");

        DeleteDeadLetterMessagesResult result;
        try
        {
            result = await _workspace.DeleteDeadLetterMessagesAsync(request, deleteCancellation.Token, progress)
                .ConfigureAwait(true);
        }
        catch (OperationCanceledException) when (
            !cancellationToken.IsCancellationRequested &&
            temporaryUnlockExpiresAt.HasValue &&
            DateTimeOffset.UtcNow >= temporaryUnlockExpiresAt.Value)
        {
            throw new InvalidOperationException(
                "Temporary write access expired during the deletion. Some messages may already have been deleted; search again.");
        }

        ApplyDeletedMessages(connectedProfileId, result);
        _backupsLoaded = false;
        if (CurrentPage == NavigationPage.Backups && _backupRepository is not null)
        {
            _pendingBackupRefresh = true;
        }

        var summary = $"{result.DeletedCount:N0} of {marked.Length:N0} deleted" +
                      (result.NotFoundCount > 0 ? $" · {result.NotFoundCount:N0} not found" : string.Empty) +
                      (result.FailedCount > 0 ? $" · {result.FailedCount:N0} failed" : string.Empty) +
                      (result.CancelledCount > 0 ? $" · {result.CancelledCount:N0} not processed (cancelled)" : string.Empty);
        StatusText = $"Selected messages: {summary}";
        MessageListTitle = $"Backup saved to {result.BackupDirectory}";
        AddActivity(
            result.FailedCount == 0 ? "Warning" : "Error",
            result.FailedCount == 0 ? "Selected dead letters backed up and deleted" : "Partial deletion of selected dead letters",
            $"{summary} · {result.BackupDirectory}");

        var problem = result.Messages.FirstOrDefault(message =>
            message.Outcome is DeadLetterMessageDeletionOutcome.Failed or DeadLetterMessageDeletionOutcome.NotFound);
        if (problem is not null)
        {
            var detail = SanitizeException(new InvalidOperationException(problem.Detail ?? "Unknown reason."));
            ErrorText = $"{summary}. Sequence {problem.Message.SequenceNumber} in {problem.Message.Source.DisplayName}: {detail} " +
                        $"The ticked messages that were not deleted are still listed.";
        }
    }

    private void ApplyDeletedMessages(Guid profileId, DeleteDeadLetterMessagesResult result) =>
        ApplyDeletedMessages(profileId, result.Messages
            .Where(message => message.Outcome == DeadLetterMessageDeletionOutcome.Deleted)
            .Select(message => message.Message));

    /// <summary>Drops removed messages from the list and lowers the scanned DLQ counters to match.</summary>
    private void ApplyDeletedMessages(Guid profileId, IEnumerable<DeadLetterMessageKey> removed)
    {
        var deleted = removed
            .Select(message => (message.Source, message.SubQueue, message.SequenceNumber))
            .ToHashSet();
        foreach (var item in Messages
                     .Where(item => deleted.Contains((item.Message.Source, item.Message.SubQueue, item.Message.SequenceNumber)))
                     .ToArray())
        {
            Messages.Remove(item);
        }
        if (SelectedMessage is not null && !Messages.Contains(SelectedMessage))
        {
            SelectedMessage = Messages.FirstOrDefault();
        }

        // Keep the scanned DLQ counters in step without waiting for a rescan.
        var deletedPerQueue = deleted
            .GroupBy(key => (key.Source, key.SubQueue))
            .ToDictionary(group => group.Key, group => (long)group.Count());
        foreach (var row in DeadLetterSources
                     .Where(row => row.ProfileId == profileId &&
                                   deletedPerQueue.ContainsKey((row.Entity, row.Snapshot.SubQueue)))
                     .ToArray())
        {
            var remaining = Math.Max(0, row.Count - deletedPerQueue[(row.Entity, row.Snapshot.SubQueue)]);
            _previousDlqCounts[$"{row.ProfileId:N}|{row.Entity.Path}|{row.Snapshot.SubQueue}"] = remaining;
            var index = DeadLetterSources.IndexOf(row);
            DeadLetterSources.RemoveAt(index);
            if (remaining > 0)
            {
                DeadLetterSources.Insert(index, new DlqSourceItemViewModel(
                    row.ProfileId,
                    row.ProfileName,
                    row.EnvironmentLabel,
                    row.EnvironmentTone,
                    new DeadLetterEntitySnapshot(row.Entity, remaining, row.Count, null, row.Snapshot.SubQueue)));
            }
        }

        SortDeadLetterSources(_preferredDlqSourceProfileId, _preferredDlqSourceEntity, _preferredDlqSourceSubQueue);
        NotifyStatistics();
    }
}
