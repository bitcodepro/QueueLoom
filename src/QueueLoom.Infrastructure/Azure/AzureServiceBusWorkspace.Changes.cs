using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using Azure;
using Azure.Core;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Infrastructure.Azure;

/// <summary>Sending, resending, purging and deleting: every deletion is backed up first.</summary>
public sealed partial class AzureServiceBusWorkspace
{
    public async Task SendMessageAsync(
        SendMessageRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        EnsureWriteAllowed();

        var sender = _senders.GetOrAdd(
            request.Destination.Name,
            name => GetMessagingClient().CreateSender(name));
        var message = AzureMessageMapper.ToAzure(request.Message);

        using var batch = await sender.CreateMessageBatchAsync(cancellationToken).ConfigureAwait(false);
        if (!batch.TryAddMessage(message))
        {
            throw new InvalidOperationException(
                "The message is larger than the maximum batch/message size allowed by this Service Bus namespace.");
        }

        await sender.SendMessagesAsync(batch, cancellationToken).ConfigureAwait(false);
    }

    public Task ResubmitDeadLetterAsync(
        ResubmitDeadLetterRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.Disposition != DeadLetterDisposition.KeepOriginal)
        {
            throw new NotSupportedException(
                "The safe MVP only resends a copy. Removing the original requires a bounded PeekLock repair workflow and is intentionally disabled.");
        }

        return SendMessageAsync(new SendMessageRequest(request.Destination, request.Message), cancellationToken);
    }

    public async Task<DeadLetterPurgeResult> PurgeDeadLettersAsync(
        DeadLetterPurgeRequest request,
        CancellationToken cancellationToken = default,
        IProgress<DeadLetterPurgeProgress>? progress = null)
    {
        ArgumentNullException.ThrowIfNull(request);
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        EnsureWriteAllowed();

        var profile = GetConnectedProfile();
        var startedAt = _timeProvider.GetUtcNow();
        foreach (var source in request.Targets.Select(target => target.Source).Distinct())
        {
            // Validate the entire request before creating a backup session or deleting
            // from an earlier target. Mixed session/non-session scopes are all-or-none.
            await EnsureSessionlessMessageSourceAsync(source, cancellationToken).ConfigureAwait(false);
        }
        var backupSession = await _backupStore.CreateSessionAsync(profile, startedAt, cancellationToken)
            .ConfigureAwait(false);
        var results = new List<DeadLetterPurgeSourceResult>(request.Targets.Count);

        for (var index = 0; index < request.Targets.Count; index++)
        {
            var target = request.Targets[index];
            if (cancellationToken.IsCancellationRequested)
            {
                foreach (var pending in request.Targets.Skip(index))
                {
                    results.Add(new DeadLetterPurgeSourceResult(
                        pending.Source,
                        pending.SubQueue,
                        0,
                        "Cancelled before this source was processed."));
                }
                break;
            }
            results.Add(await PurgeSubQueueAsync(
                    target.Source,
                    target.SubQueue,
                    request.BatchSize,
                    request.MaximumMessagesPerSubQueue,
                    backupSession,
                    index + 1,
                    request.Targets.Count,
                    progress,
                    cancellationToken)
                .ConfigureAwait(false));
            await AtomicFile.WriteTextAsync(Path.Combine(backupSession.RootDirectory, "purge-result.report"),
                System.Text.Json.JsonSerializer.Serialize(new { profileId = profile.Id, startedAt,
                    updatedAt = _timeProvider.GetUtcNow(), sources = results }), CancellationToken.None).ConfigureAwait(false);
        }

        _cachedTopology = null;
        foreach (var result in results.Where(result => result.IsSuccessful))
        {
            _previousDeadLetterCounts[$"{result.Source.Path}|{result.SubQueue}"] = 0;
        }

        return new DeadLetterPurgeResult(
            profile.Id,
            startedAt,
            _timeProvider.GetUtcNow(),
            results,
            backupSession.RootDirectory);
    }

    public async Task<DeleteDeadLetterMessagesResult> DeleteDeadLetterMessagesAsync(
        DeleteDeadLetterMessagesRequest request,
        CancellationToken cancellationToken = default,
        IProgress<DeadLetterMessageDeletionProgress>? progress = null)
    {
        ArgumentNullException.ThrowIfNull(request);
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        EnsureWriteAllowed();

        var profile = GetConnectedProfile();
        var startedAt = _timeProvider.GetUtcNow();
        var groups = request.BySubQueue.ToArray();
        foreach (var source in groups.Select(group => group.Key.Source).Distinct())
        {
            await EnsureSessionlessMessageSourceAsync(source, cancellationToken).ConfigureAwait(false);
        }
        var backupSession = await _backupStore.CreateSessionAsync(profile, startedAt, cancellationToken)
            .ConfigureAwait(false);
        var results = new List<DeadLetterMessageDeletionResult>(request.Messages.Count);

        for (var index = 0; index < groups.Length; index++)
        {
            var (source, subQueue) = groups[index].Key;
            var selection = groups[index].ToArray();
            if (cancellationToken.IsCancellationRequested)
            {
                results.AddRange(groups.Skip(index).SelectMany(group => group).Select(key =>
                    new DeadLetterMessageDeletionResult(key, DeadLetterMessageDeletionOutcome.Cancelled)));
                break;
            }

            var targetNumber = index + 1;
            var deletedBefore = results.Count(result => result.Outcome == DeadLetterMessageDeletionOutcome.Deleted);
            progress?.Report(new DeadLetterMessageDeletionProgress(
                source, subQueue, targetNumber, groups.Length, 0, deletedBefore));
            try
            {
                await using var receiver = CreateDeadLetterLockReceiver(source, subQueue);
                results.AddRange(await SelectiveDeadLetterDeleter.DeleteAsync(
                        new ServiceBusDeadLetterLockReceiver(receiver),
                        selection,
                        (message, token) => backupSession.BackupAsync(message, source, subQueue, token),
                        request.BatchSize,
                        request.MaximumScannedPerSubQueue,
                        PurgeEmptyReceiveConfirmations,
                        PurgeReceiveWaitTime,
                        (scanned, deleted) => progress?.Report(new DeadLetterMessageDeletionProgress(
                            source, subQueue, targetNumber, groups.Length, scanned, deletedBefore + deleted)),
                        cancellationToken)
                    .ConfigureAwait(false));
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                var handled = results.Select(result => result.Message).ToHashSet();
                results.AddRange(selection.Where(key => !handled.Contains(key)).Select(key =>
                    new DeadLetterMessageDeletionResult(key, DeadLetterMessageDeletionOutcome.Failed, exception.Message)));
            }

            await AtomicFile.WriteTextAsync(Path.Combine(backupSession.RootDirectory, "delete-result.report"),
                System.Text.Json.JsonSerializer.Serialize(new { profileId = profile.Id, startedAt,
                    updatedAt = _timeProvider.GetUtcNow(), messages = results }), CancellationToken.None).ConfigureAwait(false);
        }

        _cachedTopology = null;
        return new DeleteDeadLetterMessagesResult(
            profile.Id,
            startedAt,
            _timeProvider.GetUtcNow(),
            results,
            backupSession.RootDirectory);
    }

    private ServiceBusReceiver CreateDeadLetterLockReceiver(ServiceBusEntityReference source, ServiceBusSubQueue subQueue)
    {
        var options = new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            PrefetchCount = 0,
            SubQueue = subQueue switch
            {
                ServiceBusSubQueue.DeadLetter => SubQueue.DeadLetter,
                ServiceBusSubQueue.TransferDeadLetter => SubQueue.TransferDeadLetter,
                _ => throw new ArgumentOutOfRangeException(nameof(subQueue), subQueue, "Only dead-letter queues can be edited.")
            }
        };
        return source.Kind switch
        {
            ServiceBusEntityKind.Queue => GetMessagingClient().CreateReceiver(source.Name, options),
            ServiceBusEntityKind.Subscription => GetMessagingClient().CreateReceiver(source.TopicName!, source.Name, options),
            _ => throw new ArgumentException("Only queues and subscriptions have dead-letter queues.", nameof(source))
        };
    }

    private async Task<DeadLetterPurgeSourceResult> PurgeSubQueueAsync(
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue,
        int batchSize,
        int maximumMessages,
        DeadLetterJsonBackupSession backupSession,
        int targetNumber,
        int targetCount,
        IProgress<DeadLetterPurgeProgress>? progress,
        CancellationToken cancellationToken)
    {
        var options = new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            // Keep at most one small work batch locked ahead. This hides receive latency
            // without building a large lock-expiry or memory backlog while JSON is written.
            PrefetchCount = 0,
            SubQueue = subQueue switch
            {
                ServiceBusSubQueue.DeadLetter => SubQueue.DeadLetter,
                ServiceBusSubQueue.TransferDeadLetter => SubQueue.TransferDeadLetter,
                _ => throw new ArgumentOutOfRangeException(nameof(subQueue), subQueue, "Unsupported purge subqueue.")
            }
        };

        long deleted = 0;
        long backedUp = 0;
        var consecutiveEmptyReceives = 0;
        try
        {
            progress?.Report(new DeadLetterPurgeProgress(
                source, subQueue, targetNumber, targetCount, backedUp, deleted, DeadLetterPurgeStage.Starting));
            await using var receiver = source.Kind switch
            {
                ServiceBusEntityKind.Queue => GetMessagingClient().CreateReceiver(source.Name, options),
                ServiceBusEntityKind.Subscription => GetMessagingClient().CreateReceiver(
                    source.TopicName!,
                    source.Name,
                    options),
                _ => throw new ArgumentException(
                    "Only queues and subscriptions can have dead letters purged.",
                    nameof(source))
            };

            while (deleted < maximumMessages)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var remaining = maximumMessages - deleted;
                var receiveCount = (int)Math.Min(batchSize, remaining);
                var messages = await receiver.ReceiveMessagesAsync(
                        receiveCount,
                        PurgeReceiveWaitTime,
                        cancellationToken)
                    .ConfigureAwait(false);
                if (messages.Count == 0)
                {
                    consecutiveEmptyReceives++;
                    if (HasConfirmedEmptyPurge(consecutiveEmptyReceives))
                    {
                        progress?.Report(new DeadLetterPurgeProgress(
                            source, subQueue, targetNumber, targetCount, backedUp, deleted, DeadLetterPurgeStage.Verifying));
                        var remainingCount = await TryReadPurgeCountAsync(source, subQueue, cancellationToken)
                            .ConfigureAwait(false);
                        progress?.Report(new DeadLetterPurgeProgress(
                            source, subQueue, targetNumber, targetCount, backedUp, deleted, DeadLetterPurgeStage.Completed));
                        // Management-plane counters are eventually consistent. A nonzero
                        // or unavailable value after two empty receive polls is advisory,
                        // not proof that settlement failed. The UI prompts a later rescan.
                        return new DeadLetterPurgeSourceResult(
                            source,
                            subQueue,
                            deleted,
                            VerificationPending: remainingCount is null or > 0);
                    }
                    continue;
                }
                consecutiveEmptyReceives = 0;

                progress?.Report(new DeadLetterPurgeProgress(
                    source, subQueue, targetNumber, targetCount, backedUp, deleted, DeadLetterPurgeStage.BackingUp));
                foreach (var backupBatch in messages.Chunk(BackupWriteConcurrency))
                {
                    await Task.WhenAll(backupBatch.Select(message =>
                            backupSession.BackupAsync(message, source, subQueue, cancellationToken)))
                        .ConfigureAwait(false);
                }
                backedUp = checked(backedUp + messages.Count);

                progress?.Report(new DeadLetterPurgeProgress(
                    source, subQueue, targetNumber, targetCount, backedUp, deleted, DeadLetterPurgeStage.Deleting));
                var settlements = await Task.WhenAll(messages.Select(async message =>
                {
                    try
                    {
                        // Once every message in this batch has a durable backup, finish settlement
                        // independently from a UI cancellation. Cancellation is honored before the
                        // next batch, preventing an unknowable half-settled batch.
                        await receiver.CompleteMessageAsync(message, CancellationToken.None).ConfigureAwait(false);
                        return (Exception?)null;
                    }
                    catch (Exception exception) when (exception is not OperationCanceledException)
                    {
                        return exception;
                    }
                })).ConfigureAwait(false);
                deleted = checked(deleted + settlements.Count(exception => exception is null));
                var settlementError = settlements.FirstOrDefault(exception => exception is not null);
                if (settlementError is not null)
                {
                    return new DeadLetterPurgeSourceResult(source, subQueue, deleted, settlementError.Message);
                }
            }

            return new DeadLetterPurgeSourceResult(
                source,
                subQueue,
                deleted,
                Error: $"Safety limit of {maximumMessages:N0} messages was reached.",
                LimitReached: true);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return new DeadLetterPurgeSourceResult(
                source,
                subQueue,
                deleted,
                $"Cancelled safely after backing up {backedUp:N0} and deleting {deleted:N0} messages.");
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return new DeadLetterPurgeSourceResult(source, subQueue, deleted, exception.Message);
        }
    }

    internal static bool HasConfirmedEmptyPurge(int consecutiveEmptyReceives) =>
        consecutiveEmptyReceives >= PurgeEmptyReceiveConfirmations;

    private async Task<long?> TryReadPurgeCountAsync(
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue,
        CancellationToken cancellationToken)
    {
        using var verification = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        verification.CancelAfter(PurgeVerificationTimeout);
        try
        {
            var counts = await GetDeadLetterCountsAsync(source, verification.Token).ConfigureAwait(false);
            return counts.Single(item => item.SubQueue == subQueue).Count;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return null;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            _ = exception;
            return null;
        }
    }
}
