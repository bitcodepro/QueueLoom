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
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Infrastructure.Azure;

/// <summary>Browsing and searching messages: peek only, nothing is locked or changed.</summary>
public sealed partial class AzureServiceBusWorkspace
{
    /// <summary>
    /// Service Bus refuses to peek an entity that auto-forwards (it holds nothing), and one that forwards its dead letters
    /// keeps none; both are said plainly, with where the messages are instead.
    /// </summary>
    private void ThrowIfForwarded(ServiceBusEntityReference source, ServiceBusSubQueue subQueue)
    {
        var (forwardTo, deadLettersTo) = source.Kind switch
        {
            ServiceBusEntityKind.Queue => _cachedTopology?.Queues.FirstOrDefault(queue =>
                string.Equals(queue.Name, source.Name, StringComparison.OrdinalIgnoreCase)) is { } queue
                ? (queue.ForwardTo, queue.ForwardDeadLettersTo)
                : (null, null),
            ServiceBusEntityKind.Subscription => _cachedTopology?.Topics
                .FirstOrDefault(topic => string.Equals(topic.Name, source.TopicName, StringComparison.OrdinalIgnoreCase))?.Subscriptions
                .FirstOrDefault(subscription => string.Equals(subscription.Name, source.Name, StringComparison.OrdinalIgnoreCase)) is { } subscription
                ? (subscription.ForwardTo, subscription.ForwardDeadLettersTo)
                : (null, null),
            _ => (null, null)
        };
        if (subQueue == ServiceBusSubQueue.Active && Forwarding.TargetName(forwardTo) is { } target)
        {
            throw new InvalidOperationException(
                $"{source.DisplayName} forwards every message to {target} at once, so it holds none. Look in {target} instead.");
        }
        if (subQueue == ServiceBusSubQueue.DeadLetter && Forwarding.TargetName(deadLettersTo) is { } deadLetterTarget)
        {
            throw new InvalidOperationException(
                $"{source.DisplayName} forwards its dead letters to {deadLetterTarget}, so its dead-letter queue stays empty. Look in {deadLetterTarget} instead.");
        }
    }

    public async Task<IReadOnlyList<BrowsedMessage>> BrowseMessagesAsync(
        BrowseMessagesRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();

        ThrowIfForwarded(request.Source, request.SubQueue);

        // Dead-letter queues never use sessions, even when their queue or subscription does. Only active
        // messages of a session-enabled entity have to be read session by session.
        if (request.SubQueue == ServiceBusSubQueue.Active &&
            await RequiresSessionAsync(request.Source, cancellationToken).ConfigureAwait(false))
        {
            return await BrowseSessionsAsync(request, cancellationToken).ConfigureAwait(false);
        }

        var client = GetMessagingClient();
        var options = new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            PrefetchCount = 0,
            SubQueue = request.SubQueue switch
            {
                ServiceBusSubQueue.Active => SubQueue.None,
                ServiceBusSubQueue.DeadLetter => SubQueue.DeadLetter,
                ServiceBusSubQueue.TransferDeadLetter => SubQueue.TransferDeadLetter,
                _ => throw new ArgumentOutOfRangeException(nameof(request), request.SubQueue, "Unsupported subqueue.")
            }
        };

        await using var receiver = request.Source.Kind switch
        {
            ServiceBusEntityKind.Queue => client.CreateReceiver(request.Source.Name, options),
            ServiceBusEntityKind.Subscription => client.CreateReceiver(
                request.Source.TopicName!,
                request.Source.Name,
                options),
            _ => throw new ArgumentException("Only queues and subscriptions can be browsed.", nameof(request))
        };

        var result = new List<BrowsedMessage>();
        var seenSequenceNumbers = new HashSet<long>();
        var nextSequenceNumber = request.FromSequenceNumber;
        var remaining = request.LoadAll ? int.MaxValue : request.MaxMessages;

        while (remaining > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var pageSize = Math.Min(BrowseBatchSize, remaining);
            var page = await receiver.PeekMessagesAsync(
                    pageSize,
                    nextSequenceNumber,
                    cancellationToken)
                .ConfigureAwait(false);
            if (page.Count == 0)
            {
                break;
            }

            foreach (var message in page)
            {
                if (seenSequenceNumbers.Add(message.SequenceNumber))
                {
                    result.Add(AzureMessageMapper.FromAzure(message, request.Source, request.SubQueue));
                    remaining--;
                    if (remaining == 0)
                    {
                        break;
                    }
                }
            }

            var lastSequenceNumber = page[^1].SequenceNumber;
            if (lastSequenceNumber == long.MaxValue)
            {
                break;
            }

            var candidate = lastSequenceNumber + 1;
            if (nextSequenceNumber.HasValue && candidate <= nextSequenceNumber.Value)
            {
                break;
            }
            nextSequenceNumber = candidate;
        }

        return Array.AsReadOnly(result.ToArray());
    }

    public async Task<DeadLetterSearchResult> SearchDeadLettersAsync(
        DeadLetterSearchRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();

        var profile = GetConnectedProfile();
        var startedAt = _timeProvider.GetUtcNow();
        var acceptedMatches = 0;
        var resultLimitReached = 0;
        using var limiter = new SemaphoreSlim(SearchConcurrency, SearchConcurrency);
        // Counters are not trusted as an exclusion filter, but they are valuable for
        // scheduling: likely non-empty sources return useful matches before stale-zero
        // probes when a namespace contains hundreds of subscriptions.
        var targetTasks = request.Targets
            .OrderByDescending(target => target.KnownMessageCount)
            .Select(target => (Target: target, Task: SearchAsync(target)))
            .ToArray();
        var sourceResults = Array.Empty<DeadLetterSearchSourceResult>();
        try
        {
            sourceResults = await Task.WhenAll(targetTasks.Select(item => item.Task)).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Preserve every source that completed before a per-environment timeout.
            // Incomplete sources remain explicit failures instead of making all useful
            // matches from the environment disappear with Task.WhenAll cancellation.
            sourceResults = targetTasks.Select(item => item.Task.IsCompletedSuccessfully
                    ? item.Task.Result
                    : new DeadLetterSearchSourceResult(
                        item.Target.Source,
                        item.Target.SubQueue,
                        0,
                        [],
                        Error: "Search timed out before this source completed."))
                .ToArray();
        }

        return new DeadLetterSearchResult(
            profile.Id,
            startedAt,
            _timeProvider.GetUtcNow(),
            sourceResults,
            resultLimitReached != 0);

        async Task<DeadLetterSearchSourceResult> SearchAsync(DeadLetterSearchTarget target)
        {
            await limiter.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await SearchTargetAsync(
                        target,
                        request,
                        message =>
                        {
                            var position = Interlocked.Increment(ref acceptedMatches);
                            if (position <= request.MaximumResults)
                            {
                                return AzureMessageMapper.FromAzure(message, target.Source, target.SubQueue);
                            }

                            Interlocked.Exchange(ref resultLimitReached, 1);
                            return null;
                        },
                        () =>
                        {
                            if (Volatile.Read(ref acceptedMatches) < request.MaximumResults)
                            {
                                return false;
                            }

                            Interlocked.Exchange(ref resultLimitReached, 1);
                            return true;
                        },
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            finally
            {
                limiter.Release();
            }
        }
    }

    private async Task<DeadLetterSearchSourceResult> SearchTargetAsync(
        DeadLetterSearchTarget target,
        DeadLetterSearchRequest request,
        Func<ServiceBusReceivedMessage, BrowsedMessage?> addMatch,
        Func<bool> shouldStop,
        CancellationToken cancellationToken)
    {
        try
        {
            var options = new ServiceBusReceiverOptions
            {
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                PrefetchCount = 0,
                SubQueue = target.SubQueue switch
                {
                    ServiceBusSubQueue.DeadLetter => SubQueue.DeadLetter,
                    ServiceBusSubQueue.TransferDeadLetter => SubQueue.TransferDeadLetter,
                    _ => throw new ArgumentOutOfRangeException(nameof(target), target.SubQueue, "Unsupported search subqueue.")
                }
            };

            await using var receiver = target.Source.Kind switch
            {
                ServiceBusEntityKind.Queue => GetMessagingClient().CreateReceiver(target.Source.Name, options),
                ServiceBusEntityKind.Subscription => GetMessagingClient().CreateReceiver(
                    target.Source.TopicName!,
                    target.Source.Name,
                    options),
                _ => throw new ArgumentException("Only queues and subscriptions can be searched.", nameof(target))
            };

            return await SearchTargetPagesAsync(
                    target,
                    request,
                    (batchSize, fromSequenceNumber, token) => receiver.PeekMessagesAsync(
                        batchSize,
                        fromSequenceNumber,
                        token),
                    addMatch,
                    shouldStop,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return new DeadLetterSearchSourceResult(
                target.Source,
                target.SubQueue,
                0,
                [],
                Error: exception.GetBaseException().Message);
        }
    }

    internal static async Task<DeadLetterSearchSourceResult> SearchTargetPagesAsync(
        DeadLetterSearchTarget target,
        DeadLetterSearchRequest request,
        Func<int, long?, CancellationToken, Task<IReadOnlyList<ServiceBusReceivedMessage>>> peekAsync,
        Func<ServiceBusReceivedMessage, BrowsedMessage?> addMatch,
        Func<bool> shouldStop,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(target);
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(peekAsync);
        ArgumentNullException.ThrowIfNull(addMatch);
        ArgumentNullException.ThrowIfNull(shouldStop);

        var matches = new List<BrowsedMessage>();
        var scanned = 0;
        try
        {
            var scan = await WalkSearchPagesAsync(
                    request.BatchSize,
                    GetSearchSafetyLimit(request, target),
                    peekAsync,
                    azureMessage =>
                    {
                        scanned = checked(scanned + 1);
                        if (!MatchesSearch(azureMessage, request.Query))
                        {
                            return;
                        }

                        var match = addMatch(azureMessage);
                        if (match is not null)
                        {
                            matches.Add(match);
                        }
                    },
                    shouldStop,
                    cancellationToken)
                .ConfigureAwait(false);
            scanned = scan.ScannedMessageCount;

            return new DeadLetterSearchSourceResult(
                target.Source,
                target.SubQueue,
                scanned,
                Array.AsReadOnly(matches.ToArray()),
                scan.SafetyLimitReached);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return new DeadLetterSearchSourceResult(
                target.Source,
                target.SubQueue,
                scanned,
                Array.AsReadOnly(matches.ToArray()),
                Error: $"Search was cancelled or timed out after inspecting {scanned:N0} messages in this source.");
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return new DeadLetterSearchSourceResult(
                target.Source,
                target.SubQueue,
                scanned,
                Array.AsReadOnly(matches.ToArray()),
                Error: exception.GetBaseException().Message);
        }
    }

    internal static async Task<SearchPageWalkResult> WalkSearchPagesAsync(
        int batchSize,
        int safetyLimit,
        Func<int, long?, CancellationToken, Task<IReadOnlyList<ServiceBusReceivedMessage>>> peekAsync,
        Action<ServiceBusReceivedMessage> visitMessage,
        Func<bool> shouldStop,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(safetyLimit, 1);
        ArgumentNullException.ThrowIfNull(peekAsync);
        ArgumentNullException.ThrowIfNull(visitMessage);
        ArgumentNullException.ThrowIfNull(shouldStop);

        var scanned = 0;
        var stopped = false;
        var exhausted = false;
        long? fromSequenceNumber = null;
        var seenSequenceNumbers = new HashSet<long>();

        while (scanned < safetyLimit)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (shouldStop())
            {
                stopped = true;
                break;
            }

            var requested = Math.Min(batchSize, safetyLimit - scanned);
            var messages = await peekAsync(requested, fromSequenceNumber, cancellationToken)
                .ConfigureAwait(false);
            if (messages.Count == 0)
            {
                exhausted = true;
                break;
            }
            if (messages.Count > requested)
            {
                throw new InvalidOperationException("Azure Service Bus returned more peeked messages than requested.");
            }

            var lastSequenceNumber = messages.Max(message => message.SequenceNumber);
            if (fromSequenceNumber.HasValue && lastSequenceNumber < fromSequenceNumber.Value)
            {
                throw new InvalidOperationException("Azure Service Bus peek pagination did not advance.");
            }

            foreach (var message in messages)
            {
                if (!seenSequenceNumbers.Add(message.SequenceNumber))
                {
                    continue;
                }

                visitMessage(message);
                scanned = checked(scanned + 1);
                if (shouldStop())
                {
                    stopped = true;
                    break;
                }
            }

            if (stopped)
            {
                break;
            }
            if (lastSequenceNumber == long.MaxValue)
            {
                exhausted = true;
                break;
            }

            fromSequenceNumber = lastSequenceNumber + 1;
        }

        var safetyLimitReached = false;
        if (!stopped && !exhausted && scanned >= safetyLimit)
        {
            var probe = await peekAsync(1, fromSequenceNumber, cancellationToken).ConfigureAwait(false);
            safetyLimitReached = probe.Count > 0;
        }

        return new SearchPageWalkResult(scanned, safetyLimitReached);
    }

    internal static int GetSearchSafetyLimit(
        DeadLetterSearchRequest request,
        DeadLetterSearchTarget target)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(target);

        // Runtime counters are eventually consistent. They are useful for progress text,
        // but a stale low count must never truncate a content search.
        return request.MaximumMessagesPerTarget;
    }

    internal readonly record struct SearchPageWalkResult(
        int ScannedMessageCount,
        bool SafetyLimitReached);

    private static bool MatchesSearch(ServiceBusReceivedMessage message, string query)
    {
        if (Contains(message.CorrelationId, query) ||
            Contains(message.MessageId, query) ||
            Contains(message.Subject, query) ||
            Contains(message.SessionId, query) ||
            Contains(message.ContentType, query) ||
            Contains(message.DeadLetterReason, query) ||
            Contains(message.DeadLetterErrorDescription, query))
        {
            return true;
        }

        foreach (var property in message.ApplicationProperties)
        {
            if (Contains(property.Key, query) ||
                Contains(Convert.ToString(property.Value, CultureInfo.InvariantCulture), query))
            {
                return true;
            }
        }

        var body = message.Body.ToMemory();
        var searchableLength = Math.Min(body.Length, AzureMessageMapper.MaxRetainedBodyBytes);
        return searchableLength > 0 && Encoding.UTF8.GetString(body.Span[..searchableLength])
            .Contains(query, StringComparison.OrdinalIgnoreCase);
    }

    private static bool Contains(string? value, string query) =>
        !string.IsNullOrEmpty(value) && value.Contains(query, StringComparison.OrdinalIgnoreCase);
}
