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
    public bool UsesSampledCounts { get; init; }

    /// <summary>Only Azure Service Bus has transfer dead-letter queues.</summary>
    /// <summary>What the service calls a queue: "queue" by default, "topic" for Kafka.</summary>
    public string QueueKindName { get; init; } = "queue";

    /// <summary>What the service calls a topic: "topic" by default, "exchange" for RabbitMQ.</summary>
    public string TopicKindName { get; init; } = "topic";

    public bool SupportsTransferDeadLetter { get; init; } = true;

    /// <summary>
    /// False where single messages cannot be removed (Kafka keeps a log and only drops it from the oldest message on):
    /// deleting ticked messages and moving them are then unavailable, while emptying a dead-letter queue still works.
    /// </summary>
    public bool CanDeleteSelectedMessages { get; init; } = true;

    /// <summary>Whether the service reports message counts at all (Google Pub/Sub does not).</summary>
    public bool HasMessageCounts { get; init; } = true;

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
