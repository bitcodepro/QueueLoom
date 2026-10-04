namespace QueueLoom.Core.ServiceBus;

/// <summary>
/// An editable application property. Value is represented as invariant text and
/// converted to <see cref="Type"/> by the infrastructure layer when sending.
/// </summary>
public sealed record MessageApplicationProperty(
    string Name,
    ApplicationPropertyType Type,
    string Value)
{
    /// <summary>
    /// The service's own type label when QueueLoom has no equivalent type, kept so a resent copy carries it again:
    /// an SNS "String.Array" (its value is the JSON array text) or another producer's custom SQS label such as
    /// "Number.1" or "String.customer". Null for QueueLoom's own types.
    /// </summary>
    public string? WireType { get; init; }
}
