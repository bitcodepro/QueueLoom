using Azure.Messaging.ServiceBus;
using QueueLoom.Core.ServiceBus;
using AzureMessageState = global::Azure.Messaging.ServiceBus.ServiceBusMessageState;
using DomainMessageState = QueueLoom.Core.ServiceBus.ServiceBusMessageState;

namespace QueueLoom.Infrastructure.Azure;

/// <summary>Cancelling scheduled messages and removing deferred ones, each backed up first.</summary>
public sealed partial class AzureServiceBusWorkspace
{
    public async Task<RemovePendingMessagesResult> RemovePendingMessagesAsync(
        IReadOnlyList<BrowsedMessage> messages,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
        {
            throw new ArgumentException("Select at least one scheduled or deferred message.", nameof(messages));
        }
        if (messages.Count > PendingMessages.MaximumMessages)
        {
            throw new ArgumentException($"At most {PendingMessages.MaximumMessages:N0} messages can be removed at once.", nameof(messages));
        }
        if (messages.Any(message => !PendingMessages.IsPending(message)))
        {
            throw new ArgumentException("Only scheduled or deferred messages can be removed this way.", nameof(messages));
        }

        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        EnsureWriteAllowed();

        var profile = GetConnectedProfile();
        var backupSession = await _backupStore.CreateSessionAsync(profile, _timeProvider.GetUtcNow(), cancellationToken)
            .ConfigureAwait(false);
        var results = new List<PendingMessageRemovalResult>(messages.Count);
        foreach (var message in messages.DistinctBy(message => (message.Source, message.SequenceNumber)))
        {
            if (cancellationToken.IsCancellationRequested)
            {
                results.Add(new PendingMessageRemovalResult(message, DeadLetterMessageDeletionOutcome.Cancelled));
                continue;
            }

            try
            {
                results.Add(message.State == DomainMessageState.Scheduled
                    ? await CancelScheduledAsync(message, backupSession, cancellationToken).ConfigureAwait(false)
                    : await RemoveDeferredAsync(message, backupSession, cancellationToken).ConfigureAwait(false));
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                results.Add(new PendingMessageRemovalResult(message, DeadLetterMessageDeletionOutcome.Cancelled));
            }
            catch (Exception exception)
            {
                results.Add(new PendingMessageRemovalResult(message, DeadLetterMessageDeletionOutcome.Failed, exception.Message));
            }
        }

        _cachedTopology = null;
        return new RemovePendingMessagesResult(results, backupSession.RootDirectory);
    }

    private async Task<PendingMessageRemovalResult> CancelScheduledAsync(
        BrowsedMessage message,
        Persistence.DeadLetterJsonBackupSession backupSession,
        CancellationToken cancellationToken)
    {
        if (message.Source.Kind == ServiceBusEntityKind.Queue)
        {
            // Check that the sequence number still belongs to the same scheduled message before cancelling it.
            await using var peeker = GetMessagingClient().CreateReceiver(message.Source.Name);
            var current = await peeker.PeekMessageAsync(message.SequenceNumber, cancellationToken).ConfigureAwait(false);
            if (current is null || current.SequenceNumber != message.SequenceNumber ||
                current.State != AzureMessageState.Scheduled || !SameMessageId(current.MessageId, message))
            {
                return new PendingMessageRemovalResult(message, DeadLetterMessageDeletionOutcome.NotFound,
                    "It is no longer scheduled: it was delivered, cancelled or expired.");
            }
            await backupSession.BackupAsync(current, message.Source, ServiceBusSubQueue.Active, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await backupSession.BackupAsync(message, cancellationToken).ConfigureAwait(false);
        }

        var scheduledIn = PendingMessages.ScheduledIn(message);
        var sender = _senders.GetOrAdd(scheduledIn.Name, name => GetMessagingClient().CreateSender(name));
        await sender.CancelScheduledMessageAsync(message.SequenceNumber, cancellationToken).ConfigureAwait(false);
        return new PendingMessageRemovalResult(message, DeadLetterMessageDeletionOutcome.Deleted);
    }

    private async Task<PendingMessageRemovalResult> RemoveDeferredAsync(
        BrowsedMessage message,
        Persistence.DeadLetterJsonBackupSession backupSession,
        CancellationToken cancellationToken)
    {
        var source = message.Source;
        ServiceBusReceiver receiver;
        if (await RequiresSessionAsync(source, cancellationToken).ConfigureAwait(false))
        {
            if (string.IsNullOrEmpty(message.Properties.SessionId))
            {
                return new PendingMessageRemovalResult(message, DeadLetterMessageDeletionOutcome.Failed,
                    "The message has no session ID, so its session cannot be opened.");
            }
            var options = new ServiceBusSessionReceiverOptions { ReceiveMode = ServiceBusReceiveMode.PeekLock };
            receiver = source.Kind == ServiceBusEntityKind.Queue
                ? await GetMessagingClient().AcceptSessionAsync(source.Name, message.Properties.SessionId, options, cancellationToken)
                    .ConfigureAwait(false)
                : await GetMessagingClient().AcceptSessionAsync(source.TopicName!, source.Name, message.Properties.SessionId, options, cancellationToken)
                    .ConfigureAwait(false);
        }
        else
        {
            var options = new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.PeekLock, PrefetchCount = 0 };
            receiver = source.Kind == ServiceBusEntityKind.Queue
                ? GetMessagingClient().CreateReceiver(source.Name, options)
                : GetMessagingClient().CreateReceiver(source.TopicName!, source.Name, options);
        }

        await using (receiver.ConfigureAwait(false))
        {
            IReadOnlyList<ServiceBusReceivedMessage> received;
            try
            {
                received = await receiver.ReceiveDeferredMessagesAsync([message.SequenceNumber], cancellationToken).ConfigureAwait(false);
            }
            catch (ServiceBusException exception) when (exception.Reason == ServiceBusFailureReason.MessageNotFound)
            {
                return new PendingMessageRemovalResult(message, DeadLetterMessageDeletionOutcome.NotFound,
                    "It is no longer deferred: a receiver already took it, or it expired.");
            }

            var deferred = received.FirstOrDefault(item => item.SequenceNumber == message.SequenceNumber);
            if (deferred is null || !SameMessageId(deferred.MessageId, message))
            {
                // Leaving the lock to expire returns a different message to its deferred state unchanged.
                return new PendingMessageRemovalResult(message, DeadLetterMessageDeletionOutcome.NotFound,
                    "The sequence number now belongs to another message.");
            }

            try
            {
                await backupSession.BackupAsync(deferred, source, ServiceBusSubQueue.Active, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                // Without a backup the message stays: release the lock so its receiver can still take it.
                await receiver.AbandonMessageAsync(deferred, cancellationToken: CancellationToken.None).ConfigureAwait(false);
                throw;
            }
            await receiver.CompleteMessageAsync(deferred, CancellationToken.None).ConfigureAwait(false);
            return new PendingMessageRemovalResult(message, DeadLetterMessageDeletionOutcome.Deleted);
        }
    }

    private static bool SameMessageId(string? actual, BrowsedMessage expected) =>
        string.IsNullOrEmpty(expected.Properties.MessageId) || string.Equals(actual, expected.Properties.MessageId, StringComparison.Ordinal);
}
