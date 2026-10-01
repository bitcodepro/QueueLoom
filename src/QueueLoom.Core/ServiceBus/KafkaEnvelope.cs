namespace QueueLoom.Core.ServiceBus;

/// <summary>Kafka wire metadata that cannot be represented by the editable text projection.</summary>
public sealed record KafkaRawHeader(string Name, byte[]? Value);

public sealed record KafkaEnvelope(
    byte[]? Key,
    bool IsTombstone,
    IReadOnlyList<KafkaRawHeader> Headers,
    EditableMessageProperties OriginalProperties,
    IReadOnlyList<MessageApplicationProperty> OriginalApplicationProperties);
