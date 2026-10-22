namespace QueueLoom.Core.ServiceBus;

public enum MessageSchemaType
{
    Avro,
    Protobuf,
    Json
}

/// <summary>A schema from a Confluent-compatible Schema Registry, fetched by the id a message body starts with.</summary>
/// <param name="Id">The registry id written after the magic byte.</param>
/// <param name="Type">AVRO (the default), PROTOBUF or JSON.</param>
/// <param name="Text">The schema as the registry returns it: Avro JSON, a .proto file or a JSON Schema.</param>
public sealed record MessageSchema(int Id, MessageSchemaType Type, string Text)
{
    /// <summary>
    /// The names of the schemas this one references (types registered under other subjects). QueueLoom does not fetch
    /// them, so a body that uses them cannot be decoded with this schema alone; the note then says so instead of
    /// blaming the body.
    /// </summary>
    public IReadOnlyList<string> References { get; init; } = [];

    /// <summary>The note for a body this schema could not decode, when it references other schemas.</summary>
    internal string? UnresolvedReferencesNote => References.Count == 0
        ? null
        : $"Schema id {Id} refers to types from other subjects ({string.Join(", ", References.Take(5))}" +
          $"{(References.Count > 5 ? ", …" : string.Empty)}), which QueueLoom does not fetch yet, so the body is shown as bytes.";
}
