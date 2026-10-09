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
    /// "Number.1", "String.customer" or "Binary.png". Null for QueueLoom's own compatible base/type pairs.
    /// </summary>
    [System.Text.Json.Serialization.JsonIgnore(Condition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull)]
    public string? WireType { get; init; }
}
