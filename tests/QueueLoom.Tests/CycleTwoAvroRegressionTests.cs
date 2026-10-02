using QueueLoom.App.ViewModels;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class BodyDecoderTests
{
    [Theory]
    [InlineData("deflate")]
    [InlineData("null")]
    public void CycleTwoAvro_OversizedSingleRecordStopsAtDecodedLimit(string codec)
    {
        var body = AvroFile("\"string\"", codec, 1, Str(new string('x', BodyDecoder.MaximumDecodedBytes + 1)));
        if (codec == "deflate") Assert.True(body.Length < 1024 * 1024);
        var message = new BrowsedMessage(ServiceBusEntityReference.Queue("isolated"), ServiceBusSubQueue.Active, 1, body, EditableMessageProperties.Empty);
        var vm = new MessageItemViewModel(message);
        Assert.True(vm.HasDecodedBody);
        Assert.False(vm.DecodedIsJson);
        Assert.Contains("16 MiB", vm.DecodedNote, StringComparison.Ordinal);
        Assert.True(vm.DecodedText.Length < 400_000);
        Assert.False(MessageSearchQuery.Parse("$.value").Matches(message));
    }

    [Fact]
    public void CycleTwoAvro_JsonExpansionAlsoStopsAtDecodedLimit()
    {
        // Avro control bytes become six-character JSON escapes, while compressed input stays small.
        var body = AvroFile("\"string\"", "deflate", 1, Str(new string('\u0001', BodyDecoder.MaximumDecodedBytes / 4)));
        var decoded = BodyDecoder.Decode(body)!;
        Assert.False(decoded.IsJson);
        Assert.Contains("16 MiB", decoded.Note, StringComparison.Ordinal);
        Assert.True(decoded.Text.Length < 400_000);
    }

    [Theory]
    [InlineData("deflate")]
    [InlineData("null")]
    public void CycleTwoAvro_SmallRecordRemainsReadable(string codec)
    {
        var decoded = BodyDecoder.Decode(AvroFile("\"string\"", codec, 1, Str("small control")))!;
        Assert.True(decoded.IsJson);
        Assert.Contains("small control", decoded.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void CycleTwoAvro_RegistryArrayExpansionStopsAtTheSharedLimit()
    {
        var schema = new MessageSchema(1, MessageSchemaType.Avro, """{"type":"array","items":"null"}""");
        byte[] body = [0, 0, 0, 0, 1, .. Long(BodyDecoder.MaximumDecodedBytes / 4), .. Long(0)];
        var decoded = BodyDecoder.Decode(body, schema: schema)!;
        Assert.False(decoded.IsJson);
        Assert.Contains("16 MiB", decoded.Note, StringComparison.Ordinal);
        Assert.True(decoded.Text.Length < 400_000);
    }
}
