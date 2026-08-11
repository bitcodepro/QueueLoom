namespace QueueLoom.Core.ServiceBus;

public enum ServiceBusMessageState
{
    Unknown,
    Active,
    Scheduled,
    Deferred
}
