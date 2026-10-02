namespace QueueLoom.Core.ServiceBus;

/// <summary>A prepared, resumable batch of message copies bound to one environment and destination.</summary>
public sealed record ReplayPlan(
    Guid Id,
    Guid ProfileId,
    ServiceBusEntityReference Destination,
    DateTimeOffset CreatedAt,
    int Count,
    int MessagesPerSecond,
    bool PreserveMessageIds,
    string? Namespace = null,
    string? ConfigurationIdentity = null);
