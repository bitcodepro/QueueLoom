using System.Text;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class DecoderFollowUpTests
{
    // A readable body whose only hint is a message type (a "type" property text senders set routinely) is not turned
    // into schemaless Protobuf guesses; a Protobuf content type still allows them, and so does a loaded schema.
    [Theory]
    [InlineData("text/plain", false)]
    [InlineData("application/x-protobuf", true)]
    public void ReadableTextIsNotGuessedAsProtobufFromAPropertyHint(string contentType, bool protobuf)
    {
        // A newline (field 1, length-delimited), "A" (length 65) and 65 letters: complete wire data that is also text.
        var body = Encoding.UTF8.GetBytes("\nA" + new string('x', 65));
        var decoded = BodyDecoder.Decode(body, contentType, null, ProtoSchemaSet.Empty, "OrderCreated");

        Assert.Equal(protobuf, decoded?.Steps.Any(step => step.StartsWith("Protobuf", StringComparison.Ordinal)) == true);
    }

    // A lenient producer refers to a type of another namespace by its short name. Exactly one type has that name, so
    // it is used, and the record decodes instead of failing with "unknown type".
    [Fact]
    public void AvroUsesAUniqueShortNameFromAnotherNamespace()
    {
        const string definition = """
            {"type":"record","name":"Order","namespace":"shop","fields":[
              {"name":"item","type":{"type":"record","name":"Item","namespace":"other","fields":[{"name":"name","type":"string"}]}},
              {"name":"again","type":"Item"}]}
            """;
        byte[] payload = [0x02, (byte)'A', 0x02, (byte)'B'];

        var decoded = BodyDecoder.Decode(new byte[] { 0, 0, 0, 0, 7 }.Concat(payload).ToArray(),
            schema: new MessageSchema(7, MessageSchemaType.Avro, definition));

        Assert.NotNull(decoded);
        Assert.True(decoded.IsJson);
        Assert.Contains("\"again\"", decoded.Text, StringComparison.Ordinal);
        Assert.Contains("\"B\"", decoded.Text, StringComparison.Ordinal);
    }
}
