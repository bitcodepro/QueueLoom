namespace QueueLoom.Core.ServiceBus;

public sealed record ServiceBusSubscription(
    string TopicName,
    string Name,
    ServiceBusEntityRuntime Runtime,
    ServiceBusEntityStatus Status = ServiceBusEntityStatus.Unknown,
    bool RequiresSession = false)
{
    public ServiceBusEntityReference Reference =>
        ServiceBusEntityReference.Subscription(TopicName, Name);
}
