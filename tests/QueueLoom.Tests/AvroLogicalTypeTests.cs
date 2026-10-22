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

    // An invalid decimal annotation is ignored and the underlying value shown (Avro specification, logical types):
    // missing or non-positive precision, a scale above the precision, a negative or non-numeric scale.
    [Theory]
    [InlineData("""{"type":"bytes","logicalType":"decimal","scale":2}""")]
    [InlineData("""{"type":"bytes","logicalType":"decimal","precision":0,"scale":0}""")]
    [InlineData("""{"type":"bytes","logicalType":"decimal","precision":"10","scale":2}""")]
    [InlineData("""{"type":"bytes","logicalType":"decimal","precision":2,"scale":3}""")]
    [InlineData("""{"type":"bytes","logicalType":"decimal","precision":10,"scale":-1}""")]
    [InlineData("""{"type":"bytes","logicalType":"decimal","precision":10,"scale":"2"}""")]
    public void AnInvalidDecimalAnnotationShowsTheUnderlyingBytes(string schemaJson) =>
        Assert.Equal("\"MDk=\"", DecodeAvro(schemaJson, [0x04, 0x30, 0x39]));

    [Fact]
    public void ADecimalWithoutScaleIsAWholeNumber() =>
        Assert.Equal("\"12345\"", DecodeAvro("""{"type":"bytes","logicalType":"decimal","precision":10}""", [0x04, 0x30, 0x39]));

    // A time of day outside [0, one day) shows the underlying number, including values whose conversion to ticks
    // would overflow (long.MaxValue, and 1844674407370955162, which wrapped to 4 ticks and showed a wrong time).
    [Theory]
    [InlineData("""{"type":"long","logicalType":"time-micros"}""", 86_399_999_999L, "\"23:59:59.999999\"")]
    [InlineData("""{"type":"long","logicalType":"time-micros"}""", 86_400_000_000L, "86400000000")]
    [InlineData("""{"type":"long","logicalType":"time-micros"}""", -1L, "-1")]
    [InlineData("""{"type":"long","logicalType":"time-micros"}""", long.MaxValue, "9223372036854775807")]
    [InlineData("""{"type":"long","logicalType":"time-micros"}""", 1_844_674_407_370_955_162L, "1844674407370955162")]
    [InlineData("""{"type":"int","logicalType":"time-millis"}""", 86_399_999L, "\"23:59:59.999\"")]
    [InlineData("""{"type":"int","logicalType":"time-millis"}""", 86_400_000L, "86400000")]
    public void ATimeOfDayOutsideOneDayShowsTheNumber(string schemaJson, long value, string expected) =>
        Assert.Equal(expected, DecodeAvro(schemaJson, Long(value)));

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
