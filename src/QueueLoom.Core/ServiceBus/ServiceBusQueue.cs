namespace QueueLoom.Core.ServiceBus;

public sealed record ServiceBusQueue(
    string Name,
    ServiceBusEntityRuntime Runtime,
    ServiceBusEntityStatus Status = ServiceBusEntityStatus.Unknown,
    bool RequiresSession = false)
{
    public ServiceBusEntityReference Reference => ServiceBusEntityReference.Queue(Name);
}
