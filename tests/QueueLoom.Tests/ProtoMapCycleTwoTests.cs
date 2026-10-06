using System.Text.Json;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Google.Protobuf.Reflection;
using Google.Protobuf.WellKnownTypes;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class ProtoMapCycleTwoTests
{
    public static TheoryData<string, string, string, string, bool> Cases
    {
        get
        {
            var cases = new TheoryData<string, string, string, string, bool>();
            foreach (var descriptor in new[] { false, true })
            {
                cases.Add("int32", "int32", "0", "0", descriptor);
                cases.Add("bool", "bool", "false", "false", descriptor);
                cases.Add("string", "string", "", "\"\"", descriptor);
                cases.Add("int64", "bytes", "0", "\"\"", descriptor);
                cases.Add("uint32", "State", "0", "\"UNKNOWN\"", descriptor);
                cases.Add("int32", "Child", "0", "{}", descriptor);
            }
            return cases;
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void BugCycleTwo_MapEntryMissingFieldsHaveTypedDefaults(string keyType, string valueType, string key, string value, bool descriptor)
    {
        var schema = descriptor ? DescriptorSchema(keyType, valueType) : ProtoSchemaSet.FromProtoFiles([("map.proto",
            $"syntax=\"proto3\"; message Example {{ map<{keyType},{valueType}> values=1; }} message Child {{ string name=1; }} enum State {{ UNKNOWN=0; READY=1; }}")]);
        var decoded = ProtoDecoder.Decode([0x0a, 0], schema.Resolve("Example")!, schema);
        Assert.NotNull(decoded);
        using var json = JsonDocument.Parse(decoded.Value.Json);
        var entry = Assert.Single(json.RootElement.GetProperty("values").EnumerateObject());
        Assert.Equal(key, entry.Name);
        Assert.Equal(value, entry.Value.GetRawText().Replace(" ", "", StringComparison.Ordinal).Replace("\n", "", StringComparison.Ordinal).Replace("\r", "", StringComparison.Ordinal));
    }

    [Fact]
    public void BugCycleTwo_GoogleRuntimeConfirmsMissingMapDefaults()
    {
        Assert.Equal(0, ReadMap(new MapField<int, int>.Codec(FieldCodec.ForInt32(8), FieldCodec.ForInt32(16), 10))[0]);
        Assert.False(ReadMap(new MapField<bool, bool>.Codec(FieldCodec.ForBool(8), FieldCodec.ForBool(16), 10))[false]);
        Assert.Equal("", ReadMap(new MapField<string, string>.Codec(FieldCodec.ForString(10), FieldCodec.ForString(18), 10))[""]);
        Assert.NotNull(ReadMap(new MapField<int, Empty>.Codec(FieldCodec.ForInt32(8), FieldCodec.ForMessage(18, Empty.Parser), 10))[0]);
    }

    [Theory]
    [InlineData("int32", "int32", "0A02100E", "0", "14")]
    [InlineData("int32", "int32", "0A020807", "7", "0")]
    [InlineData("bool", "bool", "0A021001", "false", "true")]
    [InlineData("bool", "bool", "0A020801", "true", "false")]
    [InlineData("int32", "Child", "0A0512030A0141", "0", "{\"name\":\"A\"}")]
    [InlineData("int32", "Child", "0A020807", "7", "{}")]
    public void BugCycleTwo_MapDefaultsPreserveThePresentField(string keyType, string valueType, string hex, string key, string value)
    {
        var schemas = DescriptorSchema(keyType, valueType);
        var decoded = ProtoDecoder.Decode(Convert.FromHexString(hex), schemas.Resolve("Example")!, schemas);
        Assert.NotNull(decoded);
        Assert.Equal(1, decoded.Value.Fit);
        using var json = JsonDocument.Parse(decoded.Value.Json);
        using var expected = JsonDocument.Parse(value);
        Assert.True(JsonElement.DeepEquals(expected.RootElement, json.RootElement.GetProperty("values").GetProperty(key)));
    }

    private static MapField<TKey, TValue> ReadMap<TKey, TValue>(MapField<TKey, TValue>.Codec codec) where TKey : notnull
    {
        var map = new MapField<TKey, TValue>();
        using var input = new CodedInputStream([0x0a, 0]);
        Assert.Equal(10u, input.ReadTag());
        map.AddEntriesFrom(input, codec);
        return map;
    }

    private static ProtoSchemaSet DescriptorSchema(string key, string value)
    {
        FieldDescriptorProto Field(string name, int number, string type) => new()
        {
            Name = name, Number = number, Label = FieldDescriptorProto.Types.Label.Optional,
            Type = type switch
            {
                "int32" => FieldDescriptorProto.Types.Type.Int32, "int64" => FieldDescriptorProto.Types.Type.Int64,
                "uint32" => FieldDescriptorProto.Types.Type.Uint32, "bool" => FieldDescriptorProto.Types.Type.Bool,
                "string" => FieldDescriptorProto.Types.Type.String, "bytes" => FieldDescriptorProto.Types.Type.Bytes,
                "State" => FieldDescriptorProto.Types.Type.Enum, _ => FieldDescriptorProto.Types.Type.Message
            },
            TypeName = type is "State" or "Child" ? "." + type : ""
        };
        var entry = new DescriptorProto { Name = "ValuesEntry", Options = new MessageOptions { MapEntry = true }, Field = { Field("key", 1, key), Field("value", 2, value) } };
        var message = new DescriptorProto { Name = "Example", NestedType = { entry }, Field = { new FieldDescriptorProto { Name = "values", Number = 1, Label = FieldDescriptorProto.Types.Label.Repeated, Type = FieldDescriptorProto.Types.Type.Message, TypeName = ".Example.ValuesEntry" } } };
        var file = new FileDescriptorProto { Name = "map.proto", Syntax = "proto3", MessageType = { message, new DescriptorProto { Name = "Child", Field = { Field("name", 1, "string") } } }, EnumType = { new EnumDescriptorProto { Name = "State", Value = { new EnumValueDescriptorProto { Name = "UNKNOWN", Number = 0 }, new EnumValueDescriptorProto { Name = "READY", Number = 1 } } } } };
        return ProtoSchemaSet.FromDescriptorSet(new FileDescriptorSet { File = { file } }.ToByteArray());
    }
}
