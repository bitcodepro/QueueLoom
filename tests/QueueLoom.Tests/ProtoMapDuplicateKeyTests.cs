using System.Text.Json;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

// Protobuf lets a map key appear more than once on the wire; the last value wins (as the Google runtime decodes it).
// The decoder wrote every entry, so the JSON held the same key twice: invalid for strict readers and ambiguous for the
// rest. Each key now appears once, with its last value, in the order keys were first seen.
public sealed class ProtoMapDuplicateKeyTests
{
    private static readonly ProtoSchemaSet Schema = ProtoSchemaSet.FromProtoFiles([("map.proto",
        "syntax=\"proto3\"; message Example { map<string,int32> values=1; }")]);

    private static byte[] Entry(char key, byte value) => [0x0a, 0x05, 0x0a, 0x01, (byte)key, 0x10, value];

    [Fact]
    public void ARepeatedMapKeyKeepsItsLastValueOnce()
    {
        byte[] wire = [.. Entry('a', 1), .. Entry('b', 5), .. Entry('a', 2)];

        var decoded = ProtoDecoder.Decode(wire, Schema.Resolve("Example")!, Schema);

        Assert.NotNull(decoded);
        using var json = JsonDocument.Parse(decoded.Value.Json);
        var entries = json.RootElement.GetProperty("values").EnumerateObject().Select(property => (property.Name, property.Value.GetInt32())).ToArray();
        Assert.Equal([("a", 2), ("b", 5)], entries);
    }
}
