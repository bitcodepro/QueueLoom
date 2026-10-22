using System.Reflection;
using System.Text.Json;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

// Avro logical types were shown as their underlying values: a decimal of 123.45 as "MDk=" (base64 bytes) or hex, a
// timestamp as 1700000000000000, a date as a day count. Money and time fields now read as what they mean; a value out
// of range, or a logical type on another underlying type, still shows the underlying value.
public sealed partial class BodyDecoderTests
{
    [Theory]
    [InlineData("""{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}""", new byte[] { 0x04, 0x30, 0x39 }, "\"123.45\"")]
    [InlineData("""{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}""", new byte[] { 0x04, 0xCF, 0xC7 }, "\"-123.45\"")]
    [InlineData("""{"type":"bytes","logicalType":"decimal","precision":4,"scale":3}""", new byte[] { 0x04, 0x00, 0x05 }, "\"0.005\"")]
    [InlineData("""{"type":"fixed","name":"Money","size":4,"logicalType":"decimal","precision":9,"scale":2}""", new byte[] { 0x00, 0x00, 0x30, 0x39 }, "\"123.45\"")]
    public void AvroDecimalsReadAsNumbers(string schemaJson, byte[] body, string expected) =>
        Assert.Equal(expected, DecodeAvro(schemaJson, body));

    [Theory]
    [InlineData("""{"type":"long","logicalType":"timestamp-millis"}""", 1_700_000_000_123L, "\"2023-11-14T22:13:20.123Z\"")]
    [InlineData("""{"type":"long","logicalType":"timestamp-micros"}""", 1_700_000_000_000_001L, "\"2023-11-14T22:13:20.000001Z\"")]
    [InlineData("""{"type":"long","logicalType":"local-timestamp-millis"}""", 1_700_000_000_000L, "\"2023-11-14T22:13:20\"")]
    [InlineData("""{"type":"int","logicalType":"date"}""", 19_675L, "\"2023-11-14\"")]
    [InlineData("""{"type":"int","logicalType":"time-millis"}""", 45_296_789L, "\"12:34:56.789\"")]
    [InlineData("""{"type":"long","logicalType":"timestamp-millis"}""", long.MaxValue, "9223372036854775807")]
    [InlineData("""{"type":"string","logicalType":"timestamp-millis"}""", null, "\"x\"")]
    public void AvroTimesReadAsTimes(string schemaJson, long? value, string expected)
    {
        var body = value is { } number ? Long(number) : [0x02, (byte)'x'];
        Assert.Equal(expected, DecodeAvro(schemaJson, body));
    }

    private static string DecodeAvro(string schemaJson, byte[] body)
    {
        using var schema = JsonDocument.Parse(schemaJson);
        var schemaType = typeof(BodyDecoder).GetNestedType("AvroSchema", BindingFlags.NonPublic)!;
        var readerType = typeof(BodyDecoder).GetNestedType("AvroReader", BindingFlags.NonPublic)!;
        var decoder = Activator.CreateInstance(schemaType, schema.RootElement)!;
        var reader = Activator.CreateInstance(readerType, body, 0)!;
        using var output = new MemoryStream();
        using (var writer = new Utf8JsonWriter(output))
        {
            schemaType.GetMethod("Write")!.Invoke(decoder, [writer, schema.RootElement, reader]);
        }
        return System.Text.Encoding.UTF8.GetString(output.ToArray());
    }
}
