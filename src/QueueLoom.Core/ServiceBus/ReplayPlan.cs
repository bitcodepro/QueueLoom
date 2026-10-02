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
    string? ConfigurationIdentity = null)
{
    public string Kind { get; init; } = "Replay";
    public ResendMode Mode { get; init; } = ResendMode.Copy;
}

public sealed record OperationItem(int Index, string Origin, string? MessageId, string Destination, string State, string? Detail = null)
{
    public bool CanContinue => State == "Pending";
    public bool CanRetry => State == "Rejected";
}

public sealed record OperationHistory(ReplayPlan Plan, IReadOnlyList<OperationItem> Items);

/// <summary>Only throw when the provider explicitly proves that no delivery was accepted.
/// Timeouts, connection loss and cancellation must never use this exception.</summary>
public sealed class DeliveryRejectedException(string message, Exception? innerException = null) : Exception(message, innerException);
