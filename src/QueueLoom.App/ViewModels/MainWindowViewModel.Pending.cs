using QueueLoom.App.Models;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>Cancelling scheduled and removing deferred Azure Service Bus messages that the operator ticked.</summary>
public sealed partial class MainWindowViewModel
{
    private async Task RemoveMarkedPendingMessagesAsync(IReadOnlyList<MessageItemViewModel> marked, CancellationToken cancellationToken)
    {
        var connectedProfileId = ConnectedProfileId
            ?? throw new InvalidOperationException("Connect to the messages' environment first.");
        var profile = _connectedProfile ?? throw new InvalidOperationException("Connect first.");
        if (profile.Provider != MessagingProvider.AzureServiceBus)
        {
            throw new InvalidOperationException("Only Azure Service Bus has scheduled and deferred messages.");
        }

        // Refused before the confirmation (a typed name in Production), not by the workspace after it.
        if (marked.Count > PendingMessages.MaximumMessages)
        {
            throw new InvalidOperationException(
                $"At most {PendingMessages.MaximumMessages:N0} messages can be cancelled or removed at once. Narrow the selection.");
        }

        var scheduled = marked.Count(message => message.IsScheduled);
        var deferred = marked.Count(message => message.IsDeferred);
        var sources = marked
            .GroupBy(message => message.Message.Source.DisplayName, StringComparer.Ordinal)
            .Select(group => $"• {group.Key}: {group.Count():N0}")
            .Take(15);
        var actions = string.Join("\n", new[]
        {
            scheduled > 0 ? $"• {scheduled:N0} scheduled message(s) are cancelled: they will never be delivered." : null,
            deferred > 0 ? $"• {deferred:N0} deferred message(s) are removed: the receiver that deferred them cannot take them any more." : null
        }.Where(line => line is not null));

        var confirmed = await _dialogs.ConfirmAsync(
            scheduled > 0 && deferred == 0 ? "Cancel scheduled messages" : "Remove scheduled and deferred messages",
            $"Environment: {profile.Name}\n{profile.Provider.DisplayName()}: {profile.EndpointDisplay}\n" +
            $"Messages: {marked.Count:N0} (exactly the ticked ones)\nBackup folder: {BackupRootDirectory}\n\n" +
            $"{string.Join("\n", sources)}\n\n{actions}\n\n" +
            "Each message is saved to a local backup first; it can be sent again from Backups. " +
            "Messages that were delivered in the meantime are skipped.",
            isDangerous: true,
            requiredText: profile.Environment == EnvironmentKind.Production ? profile.Name : null,
            cancellationToken: cancellationToken).ConfigureAwait(true);
        if (!confirmed)
        {
            StatusText = "Nothing was cancelled or removed";
            return;
        }
        if (!CanWrite || ConnectedProfileId != connectedProfileId)
        {
            throw new InvalidOperationException("Write access or environment changed. Review the selection again.");
        }

        RecordOperationIntent("Remove scheduled or deferred messages started",
            $"{scheduled:N0} scheduled · {deferred:N0} deferred", null);
        using var removalCancellation = CreateExpiryBoundedWriteCancellation(connectedProfileId, cancellationToken);
        var result = await _workspace.RemovePendingMessagesAsync(marked.Select(message => message.Message).ToArray(), removalCancellation.Token)
            .ConfigureAwait(true);

        var removed = result.Messages
            .Where(item => item.Outcome == DeadLetterMessageDeletionOutcome.Deleted)
            .Select(item => (item.Message.Source, item.Message.SequenceNumber))
            .ToHashSet();
        using var batch = BatchMessageUpdates();
        foreach (var item in Messages.Where(item => (item.ProfileId is null || item.ProfileId == connectedProfileId) &&
                                                    removed.Contains((item.Message.Source, item.Message.SequenceNumber))).ToArray())
        {
            Messages.Remove(item);
        }
        if (SelectedMessage is not null && !Messages.Contains(SelectedMessage))
        {
            SelectedMessage = Messages.FirstOrDefault();
        }
        _backupsLoaded = false;
        if (CurrentPage == NavigationPage.Backups && _backupRepository is not null)
        {
            _pendingBackupRefresh = true;
        }

        var summary = $"{result.RemovedCount:N0} of {marked.Count:N0} cancelled or removed" +
                      (result.NotFoundCount > 0 ? $" · {result.NotFoundCount:N0} already gone" : string.Empty) +
                      (result.FailedCount > 0 ? $" · {result.FailedCount:N0} failed" : string.Empty) +
                      (result.CancelledCount > 0 ? $" · {result.CancelledCount:N0} not processed (cancelled)" : string.Empty);
        StatusText = summary;
        MessageListTitle = $"Backup saved to {result.BackupDirectory}";
        // "Removed" only when every ticked message was; a cancelled run, failures and messages that were already gone
        // (delivered meanwhile) say so in the title, not only in the details.
        var title = result.RemovedCount == marked.Count
            ? "Scheduled or deferred messages removed"
            : result.CancelledCount > 0
                ? "Removal of scheduled or deferred messages cancelled"
                : result.RemovedCount == 0
                    ? "Scheduled or deferred messages not removed"
                    : "Partial removal of scheduled or deferred messages";
        AddActivity(result.FailedCount == 0 ? "Warning" : "Error", title, $"{summary} · {result.BackupDirectory}");

        var problem = result.Messages.FirstOrDefault(item => item.Outcome == DeadLetterMessageDeletionOutcome.Failed);
        if (problem is not null)
        {
            ErrorText = $"{summary}. Sequence {problem.Message.SequenceNumber} in {problem.Message.Source.DisplayName}: " +
                        $"{SanitizeException(new InvalidOperationException(problem.Detail ?? "Unknown reason."))} The messages that were not removed are still listed.";
        }
    }
}
