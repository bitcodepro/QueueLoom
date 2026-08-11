namespace QueueLoom.Core.ServiceBus;

public sealed record ServiceBusTopic
{
    public ServiceBusTopic(
        string name,
        ServiceBusEntityRuntime runtime,
        IEnumerable<ServiceBusSubscription>? subscriptions = null,
        ServiceBusEntityStatus status = ServiceBusEntityStatus.Unknown)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(runtime);

        Name = name.Trim();
        Runtime = runtime;
        Subscriptions = Array.AsReadOnly((subscriptions ?? []).ToArray());
        Status = status;
    }

    public string Name { get; }

    public ServiceBusEntityRuntime Runtime { get; }

    public IReadOnlyList<ServiceBusSubscription> Subscriptions { get; }

    public ServiceBusEntityStatus Status { get; }

    public int SubscriptionCount => Subscriptions.Count;

    public ServiceBusEntityReference Reference => ServiceBusEntityReference.Topic(Name);

    public ServiceBusMessageCounts AggregateSubscriptionCounts =>
        ServiceBusMessageCounts.Sum(Subscriptions.Select(subscription => subscription.Runtime.MessageCounts));
}
