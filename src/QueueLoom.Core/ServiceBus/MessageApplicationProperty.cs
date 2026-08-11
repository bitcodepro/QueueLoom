namespace QueueLoom.Core.ServiceBus;

/// <summary>
/// An editable application property. Value is represented as invariant text and
/// converted to <see cref="Type"/> by the infrastructure layer when sending.
/// </summary>
public sealed record MessageApplicationProperty(
    string Name,
    ApplicationPropertyType Type,
    string Value);
