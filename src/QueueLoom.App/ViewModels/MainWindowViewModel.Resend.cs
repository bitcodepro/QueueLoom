using QueueLoom.App.Commands;
using QueueLoom.App.Models;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

/// <summary>Resending the ticked messages, as copies or as a move that removes the originals.</summary>
public sealed partial class MainWindowViewModel
{
    public AsyncRelayCommand ResendMarkedMessagesCommand { get; private set; } = null!;

    public string ResendMarkedMessagesLabel => MarkedMessageCount switch
    {
        0 => "Resend selected…",
        1 => "Resend 1 message…",
        var count => $"Resend {count:N0} messages…"
    };

    private void InitializeResend()
    {
        ResendMarkedMessagesCommand = new AsyncRelayCommand(
            token => RunWorkspaceOperationAsync("Resending selected messages", ResendMarkedMessagesAsync, token, allowCancellation: true),
            () => !IsBusy && CanWrite && HasMarkedMessages);
    }

    private async Task ResendMarkedMessagesAsync(CancellationToken cancellationToken)
    {
        var marked = Messages.Where(message => message.IsMarked).ToArray();
        if (marked.Length == 0)
        {
            throw new InvalidOperationException("Tick the messages to resend first.");
        }
        if (marked.Length > DeadLetterResender.MaximumMessages)
        {
            throw new InvalidOperationException(
                $"At most {DeadLetterResender.MaximumMessages:N0} messages can be resent at once. Narrow the selection.");
        }
        if (!CanWrite)
        {
            throw new InvalidOperationException("Unlock write access before resending messages.");
        }

        var connectedProfileId = ConnectedProfileId
            ?? throw new InvalidOperationException("Connect to the messages' environment first.");
        if (marked.Any(message => message.ProfileId is { } profileId && profileId != connectedProfileId))
        {
            throw new InvalidOperationException(
                "Some ticked messages belong to another environment. Connect that environment and search again.");
        }
        if (marked.Any(message => message.IsPending))
        {
            throw new InvalidOperationException(
                "Scheduled and deferred messages are not resent: they are still in their queue. Untick them.");
        }
        if (marked.Any(message => message.Message.IsBodyTruncated))
        {
            throw new InvalidOperationException(
                "Some ticked messages are too large to resend from QueueLoom. Untick the messages marked as truncated.");
        }

        var profile = _connectedProfile ?? throw new InvalidOperationException("Connect first.");
        var dialog = new ResendDialogViewModel(
            marked.Select(message => message.Message).ToArray(),
            Destinations.Select(destination => destination.Reference),
            profile.Name,
            requiresTypedConfirmation: profile.Environment == EnvironmentKind.Production,
            canRemoveOriginals: CanDeleteSelectedMessages);
        var options = await _dialogs.ChooseResendOptionsAsync(dialog, cancellationToken).ConfigureAwait(true);
        if (options is null)
        {
            StatusText = "Resend cancelled before any message was sent";
            return;
        }
        if (!CanWrite || ConnectedProfileId != connectedProfileId)
        {
            throw new InvalidOperationException("Write access or environment changed. Review the resend again.");
        }

        var items = marked
            .Select(message => new ResendItem(
                message.Message,
                options.Destination ?? DeadLetterResender.OriginalDestination(message.Message.Source),
                message.Message.CreateDraft()))
            .ToArray();
        RecordOperationIntent(
            options.Mode == ResendMode.Move ? "Move selected messages started" : "Resend selected messages started",
            $"{items.Length:N0} messages · {options.Destination?.DisplayName ?? "back to their sources"}",
            options.Destination);

        var progress = new Progress<ResendProgress>(update =>
            StatusText = $"Sent {update.Processed:N0} of {update.Total:N0}" +
                         (update.Failed > 0 ? $" · {update.Failed:N0} failed" : string.Empty));
        var result = await DeadLetterResender.ResendAsync(
                _workspace, items, options.Mode, options.MessagesPerSecond, progress, cancellationToken)
            .ConfigureAwait(true);

        RemoveResentOriginals(result);
        foreach (var item in marked.Where(item => Messages.Contains(item)))
        {
            item.IsMarked = false;
        }

        var summary = $"{result.SentCount:N0} of {items.Length:N0} sent" +
                      (options.Mode == ResendMode.Move ? $" · {result.MovedCount:N0} originals removed" : string.Empty) +
                      (result.OriginalsKeptCount > 0 ? $" · {result.OriginalsKeptCount:N0} originals kept" : string.Empty) +
                      (result.FailedCount > 0 ? $" · {result.FailedCount:N0} failed" : string.Empty) +
                      (result.CancelledCount > 0 ? $" · {result.CancelledCount:N0} not sent (cancelled)" : string.Empty);
        StatusText = $"Resend: {summary}";
        AddActivity(
            result.FailedCount == 0 && result.OriginalsKeptCount == 0 ? "Success" : "Warning",
            options.Mode == ResendMode.Move ? "Selected messages moved" : "Selected messages resent",
            summary + (result.BackupDirectory is null ? string.Empty : $" · backup {result.BackupDirectory}"),
            options.Destination);

        var problem = result.Items.FirstOrDefault(item => item.Outcome is ResendOutcome.Failed or ResendOutcome.SentOriginalKept);
        if (problem is not null)
        {
            ErrorText = $"{summary}. {problem.Item.Original.Properties.MessageId ?? "A message"}: " +
                        $"{SanitizeException(new InvalidOperationException(problem.Detail ?? "Unknown reason."))}";
        }
    }

    /// <summary>Removes moved originals from the list and the DLQ counters, and marks backups as stale.</summary>
    private void RemoveResentOriginals(ResendResult result)
    {
        var moved = result.Items
            .Where(item => item.Outcome == ResendOutcome.Moved)
            .Select(item => item.Item.Key)
            .ToArray();
        if (moved.Length == 0 || ConnectedProfileId is not { } profileId)
        {
            return;
        }

        ApplyDeletedMessages(profileId, moved);
        _backupsLoaded = false;
        if (CurrentPage == NavigationPage.Backups && _backupRepository is not null)
        {
            _pendingBackupRefresh = true;
        }
    }
}
