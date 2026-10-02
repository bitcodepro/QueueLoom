using Azure.Messaging.ServiceBus;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Azure;

/// <summary>The PeekLock operations the selective delete needs; lets the algorithm run against a fake in tests.</summary>
internal interface IDeadLetterLockReceiver
{
    Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveAsync(int maxMessages, TimeSpan maxWaitTime, CancellationToken cancellationToken);

    Task CompleteAsync(ServiceBusReceivedMessage message);

    Task AbandonAsync(ServiceBusReceivedMessage message);
}

internal sealed class ServiceBusDeadLetterLockReceiver(ServiceBusReceiver receiver) : IDeadLetterLockReceiver
{
    public async Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveAsync(
        int maxMessages,
        TimeSpan maxWaitTime,
        CancellationToken cancellationToken) =>
        await receiver.ReceiveMessagesAsync(maxMessages, maxWaitTime, cancellationToken).ConfigureAwait(false);

    public Task CompleteAsync(ServiceBusReceivedMessage message) =>
        receiver.CompleteMessageAsync(message, CancellationToken.None);

    public Task AbandonAsync(ServiceBusReceivedMessage message) =>
        receiver.AbandonMessageAsync(message, cancellationToken: CancellationToken.None);
}

/// <summary>
/// Deletes chosen messages from one dead-letter queue. Service Bus cannot settle a message by sequence
/// number, so the queue is received in PeekLock mode: selected messages are backed up and completed,
/// every other message stays locked (so the next receive moves forward) and is abandoned unchanged at the end.
/// </summary>
internal static class SelectiveDeadLetterDeleter
{
    private const int BackupConcurrency = 4;

    public static async Task<IReadOnlyList<DeadLetterMessageDeletionResult>> DeleteAsync(
        IDeadLetterLockReceiver receiver,
        IReadOnlyCollection<DeadLetterMessageKey> selection,
        Func<ServiceBusReceivedMessage, CancellationToken, Task> backupAsync,
        int batchSize,
        int maximumScanned,
        int emptyReceiveConfirmations,
        TimeSpan receiveWaitTime,
        Action<int, int>? reportProgress,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(receiver);
        ArgumentNullException.ThrowIfNull(selection);
        ArgumentNullException.ThrowIfNull(backupAsync);

        var pending = selection.ToDictionary(key => key.SequenceNumber);
        var results = new List<DeadLetterMessageDeletionResult>(pending.Count);
        var held = new List<ServiceBusReceivedMessage>();
        var seen = new HashSet<long>();
        var scanned = 0;
        var emptyReceives = 0;
        var cancelled = false;
        string? receiveError = null;

        try
        {
            while (pending.Count > 0 && scanned < maximumScanned)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    cancelled = true;
                    break;
                }

                IReadOnlyList<ServiceBusReceivedMessage> batch;
                try
                {
                    batch = await receiver.ReceiveAsync(
                            Math.Min(batchSize, maximumScanned - scanned),
                            receiveWaitTime,
                            cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    cancelled = true;
                    break;
                }
                catch (Exception exception)
                {
                    // Earlier completions are irreversible facts, even when the next receive fails.
                    receiveError = exception.GetBaseException().Message;
                    break;
                }

                if (batch.Count == 0)
                {
                    if (++emptyReceives >= emptyReceiveConfirmations)
                    {
                        break;
                    }
                    continue;
                }
                emptyReceives = 0;

                var fresh = 0;
                var matches = new List<(ServiceBusReceivedMessage Message, DeadLetterMessageKey Key)>();
                foreach (var message in batch)
                {
                    if (!seen.Add(message.SequenceNumber))
                    {
                        // A lock expired and the message came back; keep holding it.
                        held.Add(message);
                        continue;
                    }

                    fresh++;
                    scanned++;
                    if (!pending.TryGetValue(message.SequenceNumber, out var key))
                    {
                        held.Add(message);
                    }
                    else if (key.MessageId is not null &&
                             !string.Equals(key.MessageId, message.MessageId, StringComparison.Ordinal))
                    {
                        pending.Remove(message.SequenceNumber);
                        results.Add(new DeadLetterMessageDeletionResult(
                            key,
                            DeadLetterMessageDeletionOutcome.NotFound,
                            "A different message now has this sequence number; it was left unchanged."));
                        held.Add(message);
                    }
                    else
                    {
                        pending.Remove(message.SequenceNumber);
                        matches.Add((message, key));
                    }
                }

                foreach (var chunk in matches.Chunk(BackupConcurrency))
                {
                    // Settlement is not cancellable once a backup exists, so a batch is never left half done.
                    var outcomes = await Task.WhenAll(chunk.Select(match =>
                            BackupAndCompleteAsync(receiver, match.Message, match.Key, backupAsync, held, cancellationToken)))
                        .ConfigureAwait(false);
                    results.AddRange(outcomes);
                }

                reportProgress?.Invoke(scanned, results.Count(result => result.Outcome == DeadLetterMessageDeletionOutcome.Deleted));
                if (fresh == 0)
                {
                    // Only redelivered messages: the whole queue has been seen.
                    break;
                }
            }
        }
        finally
        {
            List<ServiceBusReceivedMessage> toRelease;
            lock (held)
            {
                toRelease = [.. held];
            }
            foreach (var message in toRelease)
            {
                try
                {
                    await receiver.AbandonAsync(message).ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is not OperationCanceledException)
                {
                    // The lock already expired; the message is back in the queue unchanged either way.
                }
            }
        }

        var reason = receiveError ?? (cancelled
            ? null
            : scanned >= maximumScanned
                ? $"Not reached within the first {maximumScanned:N0} messages of this dead-letter queue; it was left unchanged."
                : "Not in the dead-letter queue any more (already deleted, resubmitted or expired).");
        foreach (var key in pending.Values)
        {
            results.Add(new DeadLetterMessageDeletionResult(
                key,
                receiveError is not null ? DeadLetterMessageDeletionOutcome.Failed : cancelled ? DeadLetterMessageDeletionOutcome.Cancelled : DeadLetterMessageDeletionOutcome.NotFound,
                reason));
        }

        return results;
    }

    private static async Task<DeadLetterMessageDeletionResult> BackupAndCompleteAsync(
        IDeadLetterLockReceiver receiver,
        ServiceBusReceivedMessage message,
        DeadLetterMessageKey key,
        Func<ServiceBusReceivedMessage, CancellationToken, Task> backupAsync,
        List<ServiceBusReceivedMessage> held,
        CancellationToken cancellationToken)
    {
        try
        {
            await backupAsync(message, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            lock (held)
            {
                held.Add(message);
            }
            return new DeadLetterMessageDeletionResult(
                key,
                DeadLetterMessageDeletionOutcome.Failed,
                $"The backup could not be written, so the message was not deleted: {exception.Message}");
        }

        try
        {
            await receiver.CompleteAsync(message).ConfigureAwait(false);
            return new DeadLetterMessageDeletionResult(key, DeadLetterMessageDeletionOutcome.Deleted);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return new DeadLetterMessageDeletionResult(
                key,
                DeadLetterMessageDeletionOutcome.Failed,
                $"Backed up, but the deletion failed; the message may still be in the queue: {exception.Message}");
        }
    }
}
