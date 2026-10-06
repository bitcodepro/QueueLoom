using Azure.Messaging.ServiceBus;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Azure;

/// <summary>The PeekLock operations the selective delete needs; lets the algorithm run against a fake in tests.</summary>
internal interface IDeadLetterLockReceiver
{
    Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveAsync(int maxMessages, TimeSpan maxWaitTime, CancellationToken cancellationToken);

    Task CompleteAsync(ServiceBusReceivedMessage message);

    Task AbandonAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken);
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

    public Task AbandonAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken) =>
        receiver.AbandonMessageAsync(message, cancellationToken: cancellationToken);
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
        CancellationToken cancellationToken,
        Action<string>? reportCleanupWarning = null)
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
        var redeliveredSinceFresh = 0;
        var locksKeptExpiring = false;
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
                if (fresh > 0)
                {
                    redeliveredSinceFresh = 0;
                }
                else if ((redeliveredSinceFresh += batch.Count) > seen.Count)
                {
                    // Redelivered messages do not prove the queue is exhausted: when locks expire mid-scan, Service Bus
                    // hands the earlier messages out again before the rest of the queue. The scan keeps holding them
                    // and goes on; it gives up only when messages come back a second time with nothing new, and then
                    // says the scan was incomplete instead of claiming the rest is gone. A queue that really ends
                    // shows up as empty receives, because every message seen is still held.
                    locksKeptExpiring = true;
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
            using var budget = new CancellationTokenSource(AzureServiceBusWorkspace.AbandonBudgetOverride.Value ?? AzureServiceBusWorkspace.AbandonBudget);
            var unreleased = 0;
            for (var index = 0; index < toRelease.Count; index++)
            {
                if (budget.IsCancellationRequested)
                {
                    unreleased += toRelease.Count - index;
                    break;
                }
                try
                {
                    // One budget for the whole cleanup, independently of caller cancellation; bound even a stuck SDK task.
                    await receiver.AbandonAsync(toRelease[index], budget.Token).WaitAsync(budget.Token).ConfigureAwait(false);
                }
                catch (Exception exception) when (exception is not OutOfMemoryException)
                {
                    // Immediate failures must not prevent later releases while there is budget remaining.
                    unreleased++;
                }
            }
            if (unreleased > 0)
                reportCleanupWarning?.Invoke($"Release of {unreleased:N0} held message lock(s) was not confirmed" +
                    (budget.IsCancellationRequested ? " within the cleanup budget" : string.Empty) +
                    ". Those locks will expire; confirmed deletions are retained.");
        }

        var reason = receiveError ?? (cancelled
            ? null
            : locksKeptExpiring
                ? "Not reached: message locks kept expiring before the scan got this far, so it was left unchanged. Try again."
            : scanned >= maximumScanned
                ? $"Not reached within the first {maximumScanned:N0} messages of this dead-letter queue; it was left unchanged."
                : "Not in the dead-letter queue any more (already deleted, resubmitted or expired).");
        foreach (var key in pending.Values)
        {
            results.Add(new DeadLetterMessageDeletionResult(
                key,
                receiveError is not null || locksKeptExpiring ? DeadLetterMessageDeletionOutcome.Failed : cancelled ? DeadLetterMessageDeletionOutcome.Cancelled : DeadLetterMessageDeletionOutcome.NotFound,
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
        catch (Exception exception)
        {
            return new DeadLetterMessageDeletionResult(
                key,
                DeadLetterMessageDeletionOutcome.Failed,
                $"Backed up, but the deletion failed; the message may still be in the queue: {exception.Message}");
        }
    }
}
