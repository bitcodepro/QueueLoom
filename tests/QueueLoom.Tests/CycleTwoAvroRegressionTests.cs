using QueueLoom.App.ViewModels;
using QueueLoom.Core.ServiceBus;
using System.Reflection;
using System.Text.Json;

namespace QueueLoom.Tests;

public sealed partial class BodyDecoderTests
{
    [Theory]
    [InlineData("null")]
    [InlineData("boolean")]
    [InlineData("int")]
    [InlineData("long")]
    [InlineData("float")]
    [InlineData("double")]
    public void CycleTwoAvro_PrimitiveArrayProcessingKeepsWriterBufferBounded(string primitive)
    {
        const int count = 100_000;
        byte[] value = primitive switch
        {
            "null" => [], "boolean" => [0], "int" or "long" => Long(12345),
            "float" => BitConverter.GetBytes(1.5f), "double" => BitConverter.GetBytes(1.5d), _ => throw new InvalidOperationException()
        };
        using var input = new MemoryStream();
        input.Write(Long(count));
        for (var i = 0; i < count; i++) input.Write(value);
        input.Write(Long(0));
        using var schema = JsonDocument.Parse($$"""{"type":"array","items":"{{primitive}}"}""");
        var schemaType = typeof(BodyDecoder).GetNestedType("AvroSchema", BindingFlags.NonPublic)!;
        var readerType = typeof(BodyDecoder).GetNestedType("AvroReader", BindingFlags.NonPublic)!;
        var decoder = Activator.CreateInstance(schemaType, schema.RootElement)!;
        var reader = Activator.CreateInstance(readerType, input.ToArray(), 0)!;
        using var output = new CycleTwoCountingStream();
        using var writer = new Utf8JsonWriter(output, new JsonWriterOptions { Indented = true });
        schemaType.GetMethod("Write")!.Invoke(decoder, [writer, schema.RootElement, reader]);
        // Inspect before Flush/Dispose: checking only the final stream cannot bound the writer's own buffer.
        Assert.InRange(writer.BytesPending, 0, 64 * 1024 + 256);
        Assert.True(output.Written > 0);
        Assert.InRange(output.LargestWrite, 1, 64 * 1024 + 256);
    }

    private sealed class CycleTwoCountingStream : Stream
    {
        public long Written { get; private set; }
        public int LargestWrite { get; private set; }
        public override void Write(byte[] buffer, int offset, int count) { Written += count; LargestWrite = Math.Max(LargestWrite, count); }
        public override void Write(ReadOnlySpan<byte> buffer) { Written += buffer.Length; LargestWrite = Math.Max(LargestWrite, buffer.Length); }
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => Written;
        public override long Position { get => Written; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
    }

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
