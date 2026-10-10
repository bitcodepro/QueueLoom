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

/// <summary>Dead-letter counters for scans and monitors.</summary>
public sealed partial class AzureServiceBusWorkspace
{
    public async Task<DeadLetterSnapshot> GetDeadLetterSnapshotAsync(
        DeadLetterMonitorScope scope,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        var profile = GetConnectedProfile();
        var sources = scope.Kind switch
        {
            DeadLetterMonitorScopeKind.SingleEntity => [scope.Entity!],
            DeadLetterMonitorScopeKind.AllMessageSources =>
                (await GetTopologyCoreAsync(forceRefresh: false, cancellationToken).ConfigureAwait(false))
                .MessageSources.ToArray(),
            _ => throw new ArgumentOutOfRangeException(nameof(scope), scope.Kind, "Unsupported monitor scope.")
        };

        using var limiter = new SemaphoreSlim(MonitorConcurrency, MonitorConcurrency);
        var tasks = sources.Select(async source =>
        {
            await limiter.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                return await GetDeadLetterCountsAsync(source, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                return new[]
                {
                    CreateSnapshot(source, ServiceBusSubQueue.DeadLetter, null, exception.Message),
                    CreateSnapshot(source, ServiceBusSubQueue.TransferDeadLetter, null, exception.Message)
                };
            }
            finally
            {
                limiter.Release();
            }
        });

        var results = await Task.WhenAll(tasks).ConfigureAwait(false);
        return new DeadLetterSnapshot(profile.Id, _timeProvider.GetUtcNow(), results.SelectMany(items => items));
    }

    private async Task<DeadLetterEntitySnapshot[]> GetDeadLetterCountsAsync(
        ServiceBusEntityReference source,
        CancellationToken cancellationToken)
    {
        if (_isEmulator)
        {
            var sampled = await SampleEmulatorCountAsync(source, SubQueue.DeadLetter, cancellationToken).ConfigureAwait(false);
            return [CreateSnapshot(source, ServiceBusSubQueue.DeadLetter, sampled, null, EmulatorSampleQuality(sampled))];
        }
        var administration = GetAdministrationClient();
        long deadLetters;
        long transferDeadLetters;

        if (source.Kind == ServiceBusEntityKind.Queue)
        {
            var response = await administration.GetQueueRuntimePropertiesAsync(source.Name, cancellationToken)
                .ConfigureAwait(false);
            deadLetters = response.Value.DeadLetterMessageCount;
            transferDeadLetters = response.Value.TransferDeadLetterMessageCount;
        }
        else if (source.Kind == ServiceBusEntityKind.Subscription)
        {
            var response = await administration.GetSubscriptionRuntimePropertiesAsync(
                    source.TopicName!,
                    source.Name,
                    cancellationToken)
                .ConfigureAwait(false);
            deadLetters = response.Value.DeadLetterMessageCount;
            transferDeadLetters = response.Value.TransferDeadLetterMessageCount;
        }
        else
        {
            throw new ArgumentException("DLQ counters only exist for queues and subscriptions.", nameof(source));
        }

        return
        [
            CreateSnapshot(source, ServiceBusSubQueue.DeadLetter, deadLetters, null) with
                { ContentMarkers = await PeekContentMarkersAsync(source, SubQueue.DeadLetter, deadLetters, cancellationToken).ConfigureAwait(false) },
            CreateSnapshot(source, ServiceBusSubQueue.TransferDeadLetter, transferDeadLetters, null) with
                { ContentMarkers = await PeekContentMarkersAsync(source, SubQueue.TransferDeadLetter, transferDeadLetters, cancellationToken).ConfigureAwait(false) }
        ];
    }

    /// <summary>
    /// The sequence numbers and message IDs of a small dead-letter queue, by peeking (no lock, no delivery count): a
    /// message replaced by another at the same count shows as a new marker. Null for an empty or larger queue, or when
    /// the peek fails, which never fails the count itself.
    /// </summary>
    private async Task<IReadOnlyCollection<string>?> PeekContentMarkersAsync(ServiceBusEntityReference source, SubQueue subQueue, long count,
        CancellationToken cancellationToken)
    {
        if (count is <= 0 or > DeadLetterEntitySnapshot.ContentMarkerLimit)
        {
            return null;
        }
        var options = new ServiceBusReceiverOptions { SubQueue = subQueue, PrefetchCount = 0 };
        var client = GetMessagingClient();
        try
        {
            await using var receiver = source.Kind == ServiceBusEntityKind.Queue
                ? client.CreateReceiver(source.Name, options)
                : client.CreateReceiver(source.TopicName!, source.Name, options);
            return await PeekContentMarkersAsync(receiver, count, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is ServiceBusException or UnauthorizedAccessException or InvalidOperationException)
        {
            return null;
        }
    }

    /// <summary>Peeks up to <paramref name="count"/> messages page by page; null unless the whole queue was seen.</summary>
    internal static async Task<IReadOnlyCollection<string>?> PeekContentMarkersAsync(ServiceBusReceiver receiver, long count,
        CancellationToken cancellationToken)
    {
        var markers = new HashSet<string>(StringComparer.Ordinal);
        long? next = null;
        while (markers.Count < count)
        {
            var page = await receiver.PeekMessagesAsync((int)Math.Min(count - markers.Count, DeadLetterEntitySnapshot.ContentMarkerLimit), next,
                cancellationToken).ConfigureAwait(false);
            if (page.Count == 0) break;
            foreach (var message in page)
            {
                markers.Add($"{message.SequenceNumber}:{message.MessageId}");
            }
            if (page[^1].SequenceNumber == long.MaxValue) break;
            next = page[^1].SequenceNumber + 1;
        }
        // A partial view would make an unseen old message look new on the next check.
        return markers.Count == count ? markers : null;
    }

    private DeadLetterEntitySnapshot CreateSnapshot(
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue,
        long? count,
        string? error,
        DeadLetterCountQuality quality = DeadLetterCountQuality.Exact)
    {
        var key = $"{source.Path}|{subQueue}";
        _previousDeadLetterCounts.TryGetValue(key, out var previous);
        if (count.HasValue)
        {
            _previousDeadLetterCounts[key] = new DeadLetterMeasurement(count.Value, quality);
        }

        return new DeadLetterEntitySnapshot(source, count, previous?.Count, error, subQueue)
            { CountQuality = quality, PreviousQuality = previous?.Quality ?? DeadLetterCountQuality.Exact };
    }
}
