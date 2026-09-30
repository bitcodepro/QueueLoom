using Azure.Messaging.ServiceBus;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Azure;

public sealed partial class AzureServiceBusWorkspace
{
    private bool _isEmulator;
    private async Task<long> SampleEmulatorCountAsync(ServiceBusEntityReference source, SubQueue subQueue, CancellationToken token)
    {
        var options = new ServiceBusReceiverOptions { SubQueue = subQueue, PrefetchCount = 0 };
        var client = GetMessagingClient();
        await using var receiver = source.Kind == ServiceBusEntityKind.Queue
            ? client.CreateReceiver(source.Name, options)
            : client.CreateReceiver(source.TopicName!, source.Name, options);
        long count = 0;
        long? cursor = null;
        while (count < 1000)
        {
            // Peek transfers payloads; keep only one small page, never acquire message locks.
            var page = await receiver.PeekMessagesAsync((int)Math.Min(25, 1000 - count), cursor, token).ConfigureAwait(false);
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
    private async Task<ServiceBusEntityRuntime> SampleEmulatorRuntimeAsync(ServiceBusEntityReference source, bool requiresSession, CancellationToken token) =>
        new(new ServiceBusMessageCounts(
            active: requiresSession ? 0 : await SampleEmulatorCountAsync(source, SubQueue.None, token).ConfigureAwait(false),
            deadLetter: await SampleEmulatorCountAsync(source, SubQueue.DeadLetter, token).ConfigureAwait(false))) { IsEmulatorSample = true };

    private async Task<ServiceBusTopology> SampleEmulatorTopologyAsync(ServiceBusTopology topology, CancellationToken token)
    {
        var queues = new List<ServiceBusQueue>();
        foreach (var queue in topology.Queues)
            queues.Add(queue with { Runtime = await SampleEmulatorRuntimeAsync(queue.Reference, queue.RequiresSession, token).ConfigureAwait(false) });
        var topics = new List<ServiceBusTopic>();
        foreach (var topic in topology.Topics)
        {
            var subscriptions = new List<ServiceBusSubscription>();
            foreach (var subscription in topic.Subscriptions)
                subscriptions.Add(subscription with
                { Runtime = await SampleEmulatorRuntimeAsync(subscription.Reference, subscription.RequiresSession, token).ConfigureAwait(false) });
            topics.Add(new ServiceBusTopic(topic.Name, topic.Runtime, subscriptions, topic.Status));
        }
        return new ServiceBusTopology(_timeProvider.GetUtcNow(), queues, topics) { UsesSampledCounts = true };
    }
}
