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
    private readonly HashSet<MessageItemViewModel> _trackedMessages = [];
    private int _markedCount;
    private int _messageBatchDepth;
    private bool _messagesChangedInBatch;

    public AsyncRelayCommand DeleteMarkedMessagesCommand { get; private set; } = null!;

    public int MarkedMessageCount => _markedCount;

    public bool HasMarkedMessages => _markedCount > 0;

    public bool HasDeletableMessages => Messages.Any(message => message.CanDelete);

    public bool HasDeadLetterMessages => Messages.Any(message => message.IsDeadLetter);

    /// <summary>Requires a service that can safely identify and remove a previously reviewed message.</summary>
    public bool CanDeleteSelectedMessages => ConnectedProvider != MessagingProvider.RabbitMq && (_topology?.CanDeleteSelectedMessages ?? true);

    public bool ShowDeleteMarkedMessages => HasDeletableMessages && CanDeleteSelectedMessages;

    /// <summary>"Delete" for dead letters; "Cancel scheduled" or "Remove deferred" when the list holds those instead.</summary>
    public string DeleteMarkedMessagesLabel
    {
        get
        {
            var marked = Messages.Where(message => message.IsMarked).ToArray();
            var pool = marked.Length > 0 ? marked : Messages.Where(message => message.CanDelete).ToArray();
            var count = marked.Length;
            if (pool.Length > 0 && pool.All(message => message.IsScheduled))
            {
                return count == 0 ? "Cancel selected…" : $"Cancel {count:N0} scheduled…";
            }
            if (pool.Length > 0 && pool.All(message => message.IsPending))
            {
                return count == 0 ? "Remove selected…" : $"Remove {count:N0} {(pool.All(message => message.IsDeferred) ? "deferred" : "pending")}…";
            }
            return count switch
            {
                0 => "Delete selected…",
                1 => "Delete 1 message…",
                _ => $"Delete {count:N0} messages…"
            };
        }
    }

    public string DeleteMarkedMessagesTip => Messages.Any(message => message.IsPending) && !HasDeadLetterMessages
        ? "Back up the ticked messages, then cancel the scheduled ones and remove the deferred ones. Requires write access."
        : "Back up and permanently delete exactly the ticked dead-letter messages. Requires write access.";

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
            using var batch = BatchMessageUpdates();
            foreach (var message in Messages)
            {
                message.IsMarked = mark;
            }
        }
    }

    private void InitializeMessageDeletion()
    {
        DeleteMarkedMessagesCommand = _commands.Create(
            token => RunWorkspaceOperationAsync("Deleting selected messages", DeleteMarkedMessagesAsync, token, allowCancellation: true),
            () => !IsBusy && CanWrite && HasMarkedMessages && CanDeleteSelectedMessages);
        Messages.CollectionChanged += OnMessagesChangedForDeletion;
    }

    private void OnMessagesChangedForDeletion(object? sender, NotifyCollectionChangedEventArgs args)
    {
        if (args.Action == NotifyCollectionChangedAction.Reset)
        {
            // Clear() does not list the removed items, so the tracked set says which ones to let go.
            foreach (var item in _trackedMessages.Where(item => !Messages.Contains(item)).ToArray())
            {
                Untrack(item);
            }
        }
        foreach (var item in args.OldItems?.OfType<MessageItemViewModel>() ?? [])
        {
            Untrack(item);
        }
        foreach (var item in args.NewItems?.OfType<MessageItemViewModel>() ?? [])
        {
            if (_trackedMessages.Add(item))
            {
                item.PropertyChanged += OnMessageMarkChanged;
                _markedCount += item.IsMarked ? 1 : 0;
            }
        }
        NotifyMarkedMessagesOrDefer();
    }

    private void Untrack(MessageItemViewModel item)
    {
        if (_trackedMessages.Remove(item))
        {
            item.PropertyChanged -= OnMessageMarkChanged;
            _markedCount -= item.IsMarked ? 1 : 0;
        }
    }

    private void OnMessageMarkChanged(object? sender, PropertyChangedEventArgs args)
    {
        if (args.PropertyName == nameof(MessageItemViewModel.IsMarked) && sender is MessageItemViewModel item)
        {
            _markedCount += item.IsMarked ? 1 : -1;
            NotifyMarkedMessagesOrDefer();
        }
    }

    /// <summary>
    /// Ticking all of 50,000 messages must not rebuild the reasons and labels 50,000 times: inside a batch the
    /// change is only noted, and the batch refreshes everything once when it ends.
    /// </summary>
    /// <summary>Replaces the listed messages in one update.</summary>
    public void ReplaceMessages(IEnumerable<MessageItemViewModel> messages)
    {
        using var batch = BatchMessageUpdates();
        Messages.Clear();
        foreach (var message in messages)
        {
            Messages.Add(message);
        }
    }

    private IDisposable BatchMessageUpdates()
    {
        _messageBatchDepth++;
        return new MessageBatch(this);
    }

    private void NotifyMarkedMessagesOrDefer()
    {
        if (_messageBatchDepth > 0)
        {
            _messagesChangedInBatch = true;
            return;
        }
        NotifyMarkedMessages();
    }

    private sealed class MessageBatch(MainWindowViewModel owner) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }
            _disposed = true;
            if (--owner._messageBatchDepth == 0 && owner._messagesChangedInBatch)
            {
                owner._messagesChangedInBatch = false;
                owner.NotifyMarkedMessages();
                owner.NotifyBrowseFeatures();
            }
        }
    }

    private void NotifyMarkedMessages()
    {
        RebuildDeadLetterReasons();
        OnPropertyChanged(nameof(MarkedMessageCount));
        OnPropertyChanged(nameof(HasMarkedMessages));
        OnPropertyChanged(nameof(HasDeletableMessages));
        OnPropertyChanged(nameof(HasDeadLetterMessages));
        OnPropertyChanged(nameof(ShowDeleteMarkedMessages));
        OnPropertyChanged(nameof(DeleteMarkedMessagesLabel));
        OnPropertyChanged(nameof(DeleteMarkedMessagesTip));
        OnPropertyChanged(nameof(ResendMarkedMessagesLabel));
        OnPropertyChanged(nameof(ExportMessagesLabel));
        ExportMessagesCommand?.NotifyCanExecuteChanged();
        OnPropertyChanged(nameof(AreAllMessagesMarked));
        DeleteMarkedMessagesCommand.NotifyCanExecuteChanged();
        ResendMarkedMessagesCommand?.NotifyCanExecuteChanged();
        OnPropertyChanged(nameof(CanCompareMarkedMessages));
        CompareMarkedMessagesCommand?.NotifyCanExecuteChanged();
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
        if (marked.Any(message => message.IsPending))
        {
            if (marked.Any(message => message.IsDeadLetter))
            {
                throw new InvalidOperationException(
                    "Dead letters and scheduled or deferred messages are removed separately. Tick only one kind.");
            }
            await RemoveMarkedPendingMessagesAsync(marked, cancellationToken).ConfigureAwait(true);
            return;
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
        using var batch = BatchMessageUpdates();
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
                    new DeadLetterEntitySnapshot(row.Entity, remaining, row.Count, null, row.Snapshot.SubQueue),
                    row.QueueKindLabel.ToLowerInvariant()));
            }
        }

        SortDeadLetterSources(_preferredDlqSourceProfileId, _preferredDlqSourceEntity, _preferredDlqSourceSubQueue);
        NotifyStatistics();
    }
}
