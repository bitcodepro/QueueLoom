using System.Text.Json;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class BodyDecoderTests
{
    [Fact]
    public void BugCycleTwo_ProtobufHintCanDecodeWithoutSchemaAndInvalidWireStillShowsText()
    {
        var decoded = BodyDecoder.Decode(new byte[] { 0x0a, 0x0a }.Concat("abcdefghij"u8.ToArray()).ToArray(), "application/x-protobuf", protos: ProtoSchemaSet.Empty);
        Assert.NotNull(decoded);
        Assert.True(decoded.IsJson);
        Assert.Contains("abcdefghij", decoded.Text, StringComparison.Ordinal);
        Assert.Null(BodyDecoder.Decode("plain text"u8.ToArray(), "application/x-protobuf", protos: ProtoSchemaSet.Empty));
    }

    [Theory]
    [InlineData("application/x-protobuf", null, false)]
    [InlineData("application/octet-stream; messageType=Event", null, false)]
    [InlineData(null, "Event", false)]
    [InlineData("application/x-protobuf", null, true)]
    public void BugCycleTwo_ExplicitProtobufHintWinsOverReadableUtf8(string? contentType, string? messageType, bool gzip)
    {
        byte[] payload = [0x0a, 0x0a, .. "abcdefghij"u8.ToArray()];
        var schemas = ProtoSchemaSet.FromProtoFiles([("event.proto", "syntax=\"proto3\"; message Event { string value=1; }")]);
        Assert.Null(BodyDecoder.Decode(payload, protos: schemas));
        var decoded = BodyDecoder.Decode(gzip ? Gzip(payload) : payload, contentType, protos: schemas, messageType: messageType);
        Assert.NotNull(decoded);
        Assert.True(decoded.IsJson);
        using var json = JsonDocument.Parse(decoded.Text);
        Assert.Equal("abcdefghij", json.RootElement.GetProperty("value").GetString());
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public void BugCycleTwo_AvroReferencesUseTheirEnclosingNamespace(bool container, bool dottedName)
    {
        const string schema = """
            {"type":"record","name":"Envelope","fields":[
              {"name":"left","type":{"type":"record","name":"Container","namespace":"left","fields":[
                {"name":"definition","type":{"type":"record","name":"Item","fields":[{"name":"value","type":"string"}]}},
                {"name":"reference","type":"Item"}]}},
              {"name":"right","type":{"type":"record","name":"Container","namespace":"right","fields":[
                {"name":"definition","type":{"type":"record","name":"Item","fields":[{"name":"value","type":"long"}]}},
                {"name":"reference","type":"Item"}]}}]}
            """;
        var definition = dottedName ? schema.Replace("\"name\":\"Container\",\"namespace\":\"right\"", "\"name\":\"right.Container\",\"namespace\":\"ignored\"", StringComparison.Ordinal) : schema;
        byte[] payload = [2, 65, 2, 66, 14, 16];
        var decoded = container
            ? BodyDecoder.Decode(AvroFile(definition, "null", 1, payload))
            : BodyDecoder.Decode(new byte[] { 0, 0, 0, 0, 7 }.Concat(payload).ToArray(), schema: new MessageSchema(7, MessageSchemaType.Avro, definition));
        Assert.NotNull(decoded);
        Assert.True(decoded.IsJson, decoded.Note);
        using var json = JsonDocument.Parse(decoded.Text);
        var record = container ? json.RootElement[0] : json.RootElement;
        Assert.Equal("A", record.GetProperty("left").GetProperty("definition").GetProperty("value").GetString());
        Assert.Equal("B", record.GetProperty("left").GetProperty("reference").GetProperty("value").GetString());
        Assert.Equal(7, record.GetProperty("right").GetProperty("definition").GetProperty("value").GetInt32());
        Assert.Equal(8, record.GetProperty("right").GetProperty("reference").GetProperty("value").GetInt32());
    }
}
