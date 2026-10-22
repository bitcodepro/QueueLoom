using System.Text.Json;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

// A registry schema can reference types registered under other subjects ("references" in GET /schemas/ids/{id}).
// QueueLoom does not fetch them, so an Avro body using such a type could not be decoded, and the note said "the body
// does not match the schema", blaming the data. The references are now read, and the note says the schema refers to
// types QueueLoom does not fetch yet.
public sealed class SchemaReferencesNoteTests
{
    private const string OrderSchema = """
        {"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"addr","type":"com.acme.Address"}]}
        """;

    [Fact]
    public void TheRegistryAnswersReferencesAreKept()
    {
        using var answer = JsonDocument.Parse(JsonSerializer.Serialize(new
        {
            schema = OrderSchema,
            references = new[] { new { name = "com.acme.Address", subject = "address-value", version = 3 } }
        }));

        var schema = SchemaRegistryClient.Parse(7, answer.RootElement)!;

        Assert.Equal(["com.acme.Address"], schema.References);
    }

    [Fact]
    public void ABodyUsingAReferencedTypeIsNotBlamed()
    {
        var schema = new MessageSchema(7, MessageSchemaType.Avro, OrderSchema) { References = ["com.acme.Address"] };

        var decoded = BodyDecoder.Decode(new byte[] { 0, 0, 0, 0, 7, 0x02, (byte)'x' }, schema: schema)!;

        Assert.DoesNotContain("does not match", decoded.Note, StringComparison.Ordinal);
        Assert.Contains("refers to types from other subjects (com.acme.Address)", decoded.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void WithoutReferencesAMismatchIsStillCalledOne()
    {
        var schema = new MessageSchema(7, MessageSchemaType.Avro, OrderSchema);

        var decoded = BodyDecoder.Decode(new byte[] { 0, 0, 0, 0, 7, 0x02, (byte)'x' }, schema: schema)!;

        Assert.Contains("does not match the schema", decoded.Note, StringComparison.Ordinal);
    }
}
