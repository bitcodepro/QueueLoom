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
            CreateSnapshot(source, ServiceBusSubQueue.DeadLetter, deadLetters, null),
            CreateSnapshot(source, ServiceBusSubQueue.TransferDeadLetter, transferDeadLetters, null)
        ];
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
