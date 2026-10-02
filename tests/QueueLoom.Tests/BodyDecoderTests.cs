using System.IO.Compression;
using System.Text;
using System.Text.Json;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed partial class BodyDecoderTests
{
    [Fact]
    public void PlainJsonAndText_NeedNoDecodedView()
    {
        Assert.Null(BodyDecoder.Decode(Encoding.UTF8.GetBytes("""{"orderId":42}""")));
        Assert.Null(BodyDecoder.Decode(Encoding.UTF8.GetBytes("Order 42 was paid")));
        Assert.Null(BodyDecoder.Decode(Encoding.UTF8.GetBytes("HelloWorld12")));
    }

    [Fact]
    public void GzipOfJson_IsUnpackedAndIndented()
    {
        var decoded = BodyDecoder.Decode(Gzip("""{"orderId":42,"items":[1,2]}"""))!;

        Assert.Equal(["gzip", "JSON"], decoded.Steps);
        Assert.True(decoded.IsJson);
        Assert.Contains("\"orderId\": 42", decoded.Text, StringComparison.Ordinal);
    }

    [Fact]
    public void Base64OfGzip_IsUnpackedStepByStep()
    {
        var body = Encoding.UTF8.GetBytes(Convert.ToBase64String(Gzip("payment failed for order 42")));

        var decoded = BodyDecoder.Decode(body)!;

        Assert.Equal("base64 → gzip → text", decoded.Summary);
        Assert.Equal("payment failed for order 42", decoded.Text);
    }

    [Fact]
    public void Protobuf_IsShownByFieldNumber()
    {
        // 1: 150, 2: "hello", 3: { 1: 1 }, 4: [7, 8]
        byte[] body = [0x08, 0x96, 0x01, 0x12, 0x05, .."hello"u8, 0x1A, 0x02, 0x08, 0x01, 0x20, 0x07, 0x20, 0x08];

        var decoded = BodyDecoder.Decode(body, "application/x-protobuf")!;

        Assert.Equal(["Protobuf (no schema)"], decoded.Steps);
        using var json = JsonDocument.Parse(decoded.Text);
        Assert.Equal(150, json.RootElement.GetProperty("1").GetInt32());
        Assert.Equal("hello", json.RootElement.GetProperty("2").GetString());
        Assert.Equal(1, json.RootElement.GetProperty("3").GetProperty("1").GetInt32());
        Assert.Equal([7, 8], json.RootElement.GetProperty("4").EnumerateArray().Select(item => item.GetInt32()));
    }

    [Fact]
    public void UnknownBinary_AfterUnpacking_IsShownAsHex()
    {
        var decoded = BodyDecoder.Decode(Gzip([0xFF, 0xFE, 0x00, 0x07]))!;

        Assert.Equal(["gzip", "binary"], decoded.Steps);
        Assert.StartsWith("00000000  FF FE 00 07", decoded.Text, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("null")]
    [InlineData("deflate")]
    public void AvroContainer_IsReadWithItsOwnSchema(string codec)
    {
        const string schema = """
            {"type":"record","name":"Order","namespace":"shop","fields":[
              {"name":"id","type":"long"},
              {"name":"customer","type":["null","string"]},
              {"name":"status","type":{"type":"enum","name":"Status","symbols":["NEW","PAID"]}},
              {"name":"lines","type":{"type":"array","items":{"type":"record","name":"Line","fields":[
                {"name":"sku","type":"string"},{"name":"price","type":"double"}]}}},
              {"name":"tags","type":{"type":"map","values":"int"}},
              {"name":"previous","type":["null","shop.Status"]}
            ]}
            """;
        var records = new List<byte>();
        records.AddRange(Long(42)); records.AddRange(Long(1)); records.AddRange(Str("Ada"));
        records.AddRange(Long(1));
        records.AddRange(Long(1)); records.AddRange(Str("SKU-1")); records.AddRange(BitConverter.GetBytes(9.5)); records.AddRange(Long(0));
        records.AddRange(Long(1)); records.AddRange(Str("priority")); records.AddRange(Long(3)); records.AddRange(Long(0));
        records.AddRange(Long(1)); records.AddRange(Long(0));
        records.AddRange(Long(43)); records.AddRange(Long(0));
        records.AddRange(Long(0));
        records.AddRange(Long(0));
        records.AddRange(Long(0));
        records.AddRange(Long(0));

        var decoded = BodyDecoder.Decode(AvroFile(schema, codec, 2, records.ToArray()))!;

        Assert.Equal(["Avro"], decoded.Steps);
        Assert.Equal("2 record(s) of Order.", decoded.Note);
        using var json = JsonDocument.Parse(decoded.Text);
        var first = json.RootElement[0];
        Assert.Equal(42, first.GetProperty("id").GetInt64());
        Assert.Equal("Ada", first.GetProperty("customer").GetString());
        Assert.Equal("PAID", first.GetProperty("status").GetString());
        Assert.Equal(9.5, first.GetProperty("lines")[0].GetProperty("price").GetDouble());
        Assert.Equal(3, first.GetProperty("tags").GetProperty("priority").GetInt32());
        Assert.Equal("NEW", first.GetProperty("previous").GetString());
        Assert.Equal(JsonValueKind.Null, json.RootElement[1].GetProperty("customer").ValueKind);
    }

    [Fact]
    public void AvroWithAnUnsupportedCodec_SaysWhy()
    {
        var decoded = BodyDecoder.Decode(AvroFile("\"long\"", "snappy", 1, [.. Long(1)]))!;

        Assert.False(decoded.IsJson);
        Assert.Contains("'snappy' cannot be read here", decoded.Note, StringComparison.Ordinal);
    }

    private static byte[] Gzip(string text) => Gzip(Encoding.UTF8.GetBytes(text));

    private static byte[] Gzip(byte[] bytes)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Fastest))
        {
            gzip.Write(bytes);
        }
        return output.ToArray();
    }

    private static byte[] AvroFile(string schema, string codec, int count, byte[] records)
    {
        var sync = Enumerable.Range(1, 16).Select(value => (byte)value).ToArray();
        var file = new List<byte>("Obj"u8.ToArray()) { 1 };
        file.AddRange(Long(2));
        file.AddRange(Str("avro.schema")); file.AddRange(Bytes(Encoding.UTF8.GetBytes(schema)));
        file.AddRange(Str("avro.codec")); file.AddRange(Bytes(Encoding.UTF8.GetBytes(codec)));
        file.AddRange(Long(0));
        file.AddRange(sync);
        var block = records;
        if (codec == "deflate")
        {
            using var output = new MemoryStream();
            using (var deflate = new DeflateStream(output, CompressionLevel.Fastest))
            {
                deflate.Write(records);
            }
            block = output.ToArray();
        }
        file.AddRange(Long(count));
        file.AddRange(Long(block.Length));
        file.AddRange(block);
        file.AddRange(sync);
        return file.ToArray();
    }

    private static byte[] Long(long value)
    {
        var zigzag = (ulong)((value << 1) ^ (value >> 63));
        var bytes = new List<byte>();
        do
        {
            var next = (byte)(zigzag & 0x7F);
            zigzag >>= 7;
            bytes.Add(zigzag == 0 ? next : (byte)(next | 0x80));
        }
        while (zigzag != 0);
        return bytes.ToArray();
    }

    private static byte[] Str(string value) => Bytes(Encoding.UTF8.GetBytes(value));

    private static byte[] Bytes(byte[] value) => [.. Long(value.Length), .. value];
}
