namespace QueueLoom.Core.ServiceBus;

public sealed record ServiceBusTopology
{
    public ServiceBusTopology(
        DateTimeOffset fetchedAt,
        IEnumerable<ServiceBusQueue>? queues = null,
        IEnumerable<ServiceBusTopic>? topics = null)
    {
        FetchedAt = fetchedAt;
        Queues = Array.AsReadOnly((queues ?? []).ToArray());
        Topics = Array.AsReadOnly((topics ?? []).ToArray());
    }

    public DateTimeOffset FetchedAt { get; }

    public IReadOnlyList<ServiceBusQueue> Queues { get; }

    public IReadOnlyList<ServiceBusTopic> Topics { get; }

    public IEnumerable<ServiceBusEntityReference> MessageSources =>
        Queues.Select(queue => queue.Reference)
            .Concat(Topics.SelectMany(topic => topic.Subscriptions.Select(subscription => subscription.Reference)));

    public IEnumerable<ServiceBusEntityReference> SendDestinations =>
        Queues.Select(queue => queue.Reference)
            .Concat(Topics.Select(topic => topic.Reference));

    public ServiceBusMessageCounts AggregateMessageCounts =>
        ServiceBusMessageCounts.Sum(
            Queues.Select(queue => queue.Runtime.MessageCounts)
                .Concat(Topics.SelectMany(topic =>
                    topic.Subscriptions.Select(subscription => subscription.Runtime.MessageCounts))));
}
