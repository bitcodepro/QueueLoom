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

    /// <summary>
    /// The referenced schemas the registry handed out (also the ones they reference in turn), by the name the schema
    /// uses for them: an Avro type's full name, or a .proto import path.
    /// </summary>
    public IReadOnlyList<ReferencedSchema> Resolved { get; init; } = [];

    /// <summary>
    /// The references, at any depth, the registry did not hand out (or that were beyond the resolution bounds). Set by
    /// the registry client; without it, the direct references that were not resolved.
    /// </summary>
    public IReadOnlyList<string>? Missing { get; init; }

    /// <summary>The references that could not be resolved.</summary>
    internal IReadOnlyList<string> Unresolved => Missing ?? References.Where(name => Resolved.All(item => item.Name != name)).ToArray();

    /// <summary>The note for a body this schema could not decode, when some references could not be fetched.</summary>
    internal string? UnresolvedReferencesNote => Unresolved is { Count: > 0 } missing
        ? $"Schema id {Id} refers to types from other subjects ({string.Join(", ", missing.Take(5))}" +
          $"{(missing.Count > 5 ? ", …" : string.Empty)}) that the Schema Registry did not hand out, so the body is shown as bytes."
        : null;
}

/// <summary>A schema another one references, as the registry returned it.</summary>
public sealed record ReferencedSchema(string Name, string Text);
