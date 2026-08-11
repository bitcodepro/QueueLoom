using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Monitoring;

public sealed record DeadLetterMonitorScope
{
    private DeadLetterMonitorScope(
        DeadLetterMonitorScopeKind kind,
        ServiceBusEntityReference? entity)
    {
        Kind = kind;
        Entity = entity;
    }

    public DeadLetterMonitorScopeKind Kind { get; }

    public ServiceBusEntityReference? Entity { get; }

    public static DeadLetterMonitorScope All { get; } =
        new(DeadLetterMonitorScopeKind.AllMessageSources, null);

    public static DeadLetterMonitorScope ForEntity(ServiceBusEntityReference entity)
    {
        ArgumentNullException.ThrowIfNull(entity);
        if (!entity.CanBrowse)
        {
            throw new ArgumentException(
                "Dead letters can only be monitored for queues or subscriptions.",
                nameof(entity));
        }

        return new DeadLetterMonitorScope(DeadLetterMonitorScopeKind.SingleEntity, entity);
    }
}
