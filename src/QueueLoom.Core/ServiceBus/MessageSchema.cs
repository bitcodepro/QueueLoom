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
public sealed record MessageSchema(int Id, MessageSchemaType Type, string Text);
