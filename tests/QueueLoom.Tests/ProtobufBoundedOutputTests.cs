using System.Diagnostics;
using System.IO.Compression;
using System.Text.Json;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class ProtobufBoundedOutputTests
{
    private static readonly byte[] Flood = Enumerable.Range(0, BodyDecoder.MaximumDecodedBytes / 2).SelectMany(_ => new byte[] { 0x08, 0x01 }).ToArray();

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SchemaLessProtobufStopsAtTheFieldBudget(bool gzip)
    {
        var body = gzip ? Gzip(Flood) : Flood;
        var watch = Stopwatch.StartNew();
        var decoded = BodyDecoder.Decode(body, "application/x-protobuf", protos: ProtoSchemaSet.Empty);
        watch.Stop();
        Assert.NotNull(decoded);
        Assert.True(decoded.IsJson);
        Assert.False(decoded.IsComplete);
        Assert.Contains("fields are shown", decoded.Note, StringComparison.Ordinal);
        Assert.True(decoded.Text.Length <= BodyDecoder.MaximumDecodedBytes / 8, $"{decoded.Text.Length:N0} chars");
        using var json = JsonDocument.Parse(decoded.Text);
        Assert.Equal(BodyDecoder.MaximumProtobufValues, json.RootElement.GetProperty("1").GetArrayLength());
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(3), watch.Elapsed.ToString());
    }

    [Fact]
    public void SchemaLessProtobufWithoutHintAlsoStops()
    {
        var decoded = BodyDecoder.Decode(Flood, protos: ProtoSchemaSet.Empty);
        Assert.NotNull(decoded);
        Assert.False(decoded.IsComplete);
        Assert.True(decoded.Text.Length <= BodyDecoder.MaximumDecodedBytes / 8);
    }

    [Fact]
    public void TypedProtobufStopsAtTheFieldBudget()
    {
        var schemas = ProtoSchemaSet.FromProtoFiles([("e.proto", "syntax=\"proto3\"; message E { repeated int32 v=1 [packed=false]; }")]);
        var decoded = BodyDecoder.Decode(Flood, "application/x-protobuf; messageType=E", protos: schemas, messageType: "E");
        Assert.NotNull(decoded);
        Assert.False(decoded.IsComplete);
        using var json = JsonDocument.Parse(decoded.Text);
        Assert.Equal(BodyDecoder.MaximumProtobufValues, json.RootElement.GetProperty("v").GetArrayLength());
    }

    [Fact]
    public void TypedPackedProtobufStopsAtTheFieldBudget()
    {
        var schemas = ProtoSchemaSet.FromProtoFiles([("e.proto", "syntax=\"proto3\"; message E { repeated int32 v=1; }")]);
        var packed = Enumerable.Repeat((byte)0x01, 1_000_000).ToArray();
        byte[] body = [0x0a, 0xC0, 0x84, 0x3D, .. packed];
        var decoded = ProtoDecoder.Decode(body, schemas.Resolve("E")!, schemas)!.Value;
        Assert.True(decoded.Truncated);
        using var json = JsonDocument.Parse(decoded.Json);
        // The field itself takes one from the shared budget.
        Assert.Equal(BodyDecoder.MaximumProtobufValues - 1, json.RootElement.GetProperty("v").GetArrayLength());
    }

    [Fact]
    public void OverlongVarintsAreRejected()
    {
        byte[] overflow = [0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F];
        var position = 1;
        Assert.False(BodyDecoder.Protobuf.TryReadVarint(overflow, ref position, out _));
        Assert.False(BodyDecoder.Protobuf.TryToJson(overflow, requireMessage: false, out _, out _));
        var schemas = ProtoSchemaSet.FromProtoFiles([("e.proto", "syntax=\"proto3\"; message E { uint64 v=1; }")]);
        Assert.Null(ProtoDecoder.Decode(overflow, schemas.Resolve("E")!, schemas));
    }

    [Fact]
    public void TheLargestVarintStillDecodes()
    {
        byte[] max = [0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01];
        Assert.True(BodyDecoder.Protobuf.TryToJson(max, requireMessage: false, out var json, out var truncated));
        Assert.False(truncated);
        using (var document = JsonDocument.Parse(json))
        {
            Assert.Equal(ulong.MaxValue, document.RootElement.GetProperty("1").GetUInt64());
        }
        var schemas = ProtoSchemaSet.FromProtoFiles([("e.proto", "syntax=\"proto3\"; message E { uint64 v=1; }")]);
        using var typed = JsonDocument.Parse(ProtoDecoder.Decode(max, schemas.Resolve("E")!, schemas)!.Value.Json);
        Assert.Equal(ulong.MaxValue, typed.RootElement.GetProperty("v").GetUInt64());
    }

    [Theory]
    [InlineData("FFFFFFFFFFFFFFFFFF7F", false)]
    [InlineData("FFFFFFFFFFFFFFFFFF02", false)]
    [InlineData("FFFFFFFFFFFFFFFFFF01", true)]
    public void AvroLongsRejectBitsPastSixtyFour(string hex, bool valid)
    {
        // A long schema: the body is one zigzag long.
        var schema = """{"type":"long"}""";
        var file = AvroFile(schema, Convert.FromHexString(hex));
        var (json, note, _) = BodyDecoder.AvroContainer.ToJson(file);
        if (valid)
        {
            Assert.Equal("[\n  -9223372036854775808\n]", json!.ReplaceLineEndings("\n"));
        }
        else
        {
            Assert.Null(json);
            Assert.Contains("too long", note, StringComparison.Ordinal);
        }
    }

    private static byte[] AvroFile(string schema, byte[] record)
    {
        static byte[] Long(long value)
        {
            var zig = (ulong)((value << 1) ^ (value >> 63));
            var bytes = new List<byte>();
            while (zig >= 0x80) { bytes.Add((byte)(zig | 0x80)); zig >>= 7; }
            bytes.Add((byte)zig);
            return [.. bytes];
        }
        var schemaBytes = System.Text.Encoding.UTF8.GetBytes(schema);
        var sync = new byte[16];
        return
        [
            .. "Obj\u0001"u8.ToArray(), .. Long(1), .. Long(11), .. "avro.schema"u8.ToArray(), .. Long(schemaBytes.Length), .. schemaBytes, .. Long(0),
            .. sync, .. Long(1), .. Long(record.Length), .. record, .. sync
        ];
    }

    private static byte[] Gzip(byte[] bytes)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Optimal))
        {
            gzip.Write(bytes);
        }
        return output.ToArray();
    }
}
