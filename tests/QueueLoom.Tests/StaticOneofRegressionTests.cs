using System.Text;
using System.Text.Json;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class StaticOneofRegressionTests
{
    private const string Proto = """
        syntax = "proto3";
        message M { oneof choice { int32 a = 1; int32 b = 2; Child c = 3; }
          Child plain = 4; oneof other { int32 d = 5; int32 e = 6; } }
        message Child { oneof inner { int32 x = 1; int32 y = 2; } repeated int32 list = 3; int32 z = 4; }
        """;

    [Theory]
    [InlineData(false, "08011002", "{\"b\":2}")]
    [InlineData(true, "08011002", "{\"b\":2}")]
    [InlineData(false, "10020801", "{\"a\":1}")]
    [InlineData(true, "10020801", "{\"a\":1}")]
    [InlineData(false, "08010803", "{\"a\":3}")]
    [InlineData(true, "08010803", "{\"a\":3}")]
    [InlineData(false, "080110020800", "{\"a\":0}")]
    [InlineData(true, "080110020800", "{\"a\":0}")]
    [InlineData(false, "0801280430051002", "{\"b\":2,\"e\":5}")]
    [InlineData(true, "0801280430051002", "{\"b\":2,\"e\":5}")]
    [InlineData(false, "1A060801180120041A0410021802", "{\"c\":{\"y\":2,\"list\":[1,2],\"z\":4}}")]
    [InlineData(true, "1A060801180120041A0410021802", "{\"c\":{\"y\":2,\"list\":[1,2],\"z\":4}}")]
    [InlineData(false, "1A02080108011A021002", "{\"c\":{\"y\":2}}")]
    [InlineData(true, "1A02080108011A021002", "{\"c\":{\"y\":2}}")]
    [InlineData(false, "22020801220410020803", "{\"plain\":{\"x\":3}}")]
    [InlineData(true, "22020801220410020803", "{\"plain\":{\"x\":3}}")]
    public void LastMemberWinsAndRepeatedEmbeddedMessagesMerge(bool descriptor, string hex, string expected)
    {
        var schemas = descriptor ? ProtoSchemaSet.FromDescriptorSet(Descriptor()) : ProtoSchemaSet.FromProtoFiles([("oneof.proto", Proto)]);
        var bytes = Convert.FromHexString(hex);
        using var actual = JsonDocument.Parse(ProtoDecoder.Decode(bytes, schemas.Resolve("M")!, schemas)!.Value.Json);
        using var wanted = JsonDocument.Parse(expected);
        Assert.Equal(wanted.RootElement.ToString(), Compact(actual.RootElement));
        // Decoding is presentation only.
        Assert.Equal(hex, Convert.ToHexString(bytes));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void BodyDecoderAndSchemaRegistryUseOneofSemantics(bool registry)
    {
        byte[] body = [8, 1, 16, 2];
        var decoded = registry
            ? BodyDecoder.Decode(new byte[] { 0, 0, 0, 0, 7, 0 }.Concat(body).ToArray(), schema: new MessageSchema(7, MessageSchemaType.Protobuf, Proto))
            : BodyDecoder.Decode(body, "application/x-protobuf; messageType=M", protos: ProtoSchemaSet.FromProtoFiles([("oneof.proto", Proto)]));
        using var json = JsonDocument.Parse(decoded!.Text);
        Assert.Equal("{\"b\":2}", Compact(json.RootElement));
    }

    private static string Compact(JsonElement element) => JsonSerializer.Serialize(element);
    private static byte[] Descriptor()
    {
        byte[] Message(string name, byte[][] fields, string[] groups) => Join(Text(1, name),
            Join(fields.Select(f => Bytes(2, f)).ToArray()), Join(groups.Select(g => Bytes(8, Text(1, g))).ToArray()));
        byte[] Field(string name, int number, int type, int? group = null, string? reference = null, bool repeated = false) =>
            Join(Text(1, name), Number(3, number), Number(4, repeated ? 3 : 1), Number(5, type),
                group is { } index ? Number(9, index) : [], reference is null ? [] : Text(6, reference));
        var m = Message("M", [Field("a", 1, 5, 0), Field("b", 2, 5, 0), Field("c", 3, 11, 0, ".Child"),
            Field("plain", 4, 11, reference: ".Child"), Field("d", 5, 5, 1), Field("e", 6, 5, 1)], ["choice", "other"]);
        var child = Message("Child", [Field("x", 1, 5, 0), Field("y", 2, 5, 0), Field("list", 3, 5, repeated: true), Field("z", 4, 5)], ["inner"]);
        return Bytes(1, Join(Text(1, "oneof.proto"), Bytes(4, m), Bytes(4, child)));
    }
    private static byte[] Text(int number, string text) => Bytes(number, Encoding.UTF8.GetBytes(text));
    private static byte[] Bytes(int number, byte[] data) => Join(Varint((number << 3) | 2), Varint(data.Length), data);
    private static byte[] Number(int number, int value) => Join(Varint(number << 3), Varint(value));
    private static byte[] Varint(int value)
    {
        var bytes = new List<byte>();
        do { bytes.Add((byte)((value & 127) | (value > 127 ? 128 : 0))); value >>= 7; } while (value > 0);
        return bytes.ToArray();
    }
    private static byte[] Join(params byte[][] parts) => parts.SelectMany(p => p).ToArray();
}
