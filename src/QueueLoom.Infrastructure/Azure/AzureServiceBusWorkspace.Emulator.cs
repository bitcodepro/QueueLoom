using Azure.Messaging.ServiceBus;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Azure;

public sealed partial class AzureServiceBusWorkspace
{
    private bool _isEmulator;

    /// <summary>How many messages the emulator (which reports no counts) is peeked to count them.</summary>
    internal const int EmulatorSampleLimit = 1_000;

    /// <summary>
    /// A peek that ran dry before the cap saw every message; one that reached the cap saw only that many, so the count is
    /// then only a lower bound.
    /// </summary>
    internal static QueueLoom.Core.Monitoring.DeadLetterCountQuality EmulatorSampleQuality(long count) =>
        count >= EmulatorSampleLimit ? QueueLoom.Core.Monitoring.DeadLetterCountQuality.LowerBound : QueueLoom.Core.Monitoring.DeadLetterCountQuality.Exact;

    private async Task<long> SampleEmulatorCountAsync(ServiceBusEntityReference source, SubQueue subQueue, CancellationToken token)
    {
        var options = new ServiceBusReceiverOptions { SubQueue = subQueue, PrefetchCount = 0 };
        var client = GetMessagingClient();
        await using var receiver = source.Kind == ServiceBusEntityKind.Queue
            ? client.CreateReceiver(source.Name, options)
            : client.CreateReceiver(source.TopicName!, source.Name, options);
        long count = 0;
        long? cursor = null;
        while (count < EmulatorSampleLimit)
        {
            // Peek transfers payloads; keep only one small page, never acquire message locks.
            var page = await receiver.PeekMessagesAsync((int)Math.Min(25, EmulatorSampleLimit - count), cursor, token).ConfigureAwait(false);
            if (page.Count == 0) break;
            count += page.Count;
            var sequence = page[^1].SequenceNumber;
            if (sequence == long.MaxValue || cursor.HasValue && sequence < cursor.Value) break;
            cursor = sequence + 1;
        }
        return count;
    }

    // Active messages of a session-enabled entity can only be peeked session by session; the sample counts
    // its dead letters, which never use sessions, and leaves the active count at zero.
    // An entity that auto-forwards holds nothing and cannot be peeked at all; one that forwards its dead letters has
    // an empty dead-letter queue.
    internal static async Task<ServiceBusEntityRuntime> SampleEmulatorRuntimeAsync(ServiceBusEntityReference source, bool requiresSession,
        string? forwardTo, string? forwardDeadLettersTo,
        Func<ServiceBusEntityReference, SubQueue, CancellationToken, Task<long>> sampleCount, CancellationToken token)
    {
        var active = requiresSession || forwardTo is not null ? 0 : await sampleCount(source, SubQueue.None, token).ConfigureAwait(false);
        var deadLetter = forwardDeadLettersTo is not null ? 0 : await sampleCount(source, SubQueue.DeadLetter, token).ConfigureAwait(false);
        return new(new ServiceBusMessageCounts(active: active, deadLetter: deadLetter))
        {
            IsEmulatorSample = true,
            // A capped sample is at least the cap, as the monitor's snapshot already says (EmulatorSampleQuality).
            DeadLetterCountIsLowerBound = EmulatorSampleQuality(deadLetter) == QueueLoom.Core.Monitoring.DeadLetterCountQuality.LowerBound
        };
    }

    private Task<ServiceBusTopology> SampleEmulatorTopologyAsync(ServiceBusTopology topology, CancellationToken token) =>
        SampleEmulatorTopologyAsync(topology, SampleEmulatorCountAsync, _timeProvider, token);

    internal static async Task<ServiceBusTopology> SampleEmulatorTopologyAsync(ServiceBusTopology topology,
        Func<ServiceBusEntityReference, SubQueue, CancellationToken, Task<long>> sampleCount,
        TimeProvider timeProvider, CancellationToken token)
    {
        var queues = new List<ServiceBusQueue>();
        foreach (var queue in topology.Queues)
        {
            try
            {
                queues.Add(queue with
                {
                    Runtime = await SampleEmulatorRuntimeAsync(queue.Reference, queue.RequiresSession, queue.ForwardTo,
                        queue.ForwardDeadLettersTo, sampleCount, token).ConfigureAwait(false)
                });
            }
            catch (ServiceBusException exception) when (exception.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
            {
                // Discovery and peeking are separate requests; omit an entity deleted between them.
            }
        }
        var topics = new List<ServiceBusTopic>();
        foreach (var topic in topology.Topics)
        {
            var subscriptions = new List<ServiceBusSubscription>();
            foreach (var subscription in topic.Subscriptions)
            {
                try
                {
                    subscriptions.Add(subscription with
                    {
                        Runtime = await SampleEmulatorRuntimeAsync(subscription.Reference, subscription.RequiresSession,
                            subscription.ForwardTo, subscription.ForwardDeadLettersTo, sampleCount, token).ConfigureAwait(false)
                    });
                }
                catch (ServiceBusException exception) when (exception.Reason == ServiceBusFailureReason.MessagingEntityNotFound)
                {
                    // A subscription (or its parent topic) disappeared after discovery.
                }
            }
            topics.Add(new ServiceBusTopic(topic.Name, topic.Runtime, subscriptions, topic.Status));
        }
        return new ServiceBusTopology(timeProvider.GetUtcNow(), queues, topics) { UsesSampledCounts = true };
    }
}
