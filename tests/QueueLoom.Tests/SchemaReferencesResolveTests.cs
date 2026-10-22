using System.Net;
using System.Text.Json;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

// A registry schema can reference types registered under other subjects (Confluent "schema references"). QueueLoom
// now fetches them (GET /subjects/{subject}/versions/{version}, and theirs in turn) and decodes with them: an Avro
// record whose field is another subject's record, and a .proto that imports another subject's file.
public sealed class SchemaReferencesResolveTests
{
    private const string Order = """
        {"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"id","type":"long"},{"name":"addr","type":"com.acme.Address"}]}
        """;
    private const string Address = """
        {"type":"record","name":"Address","namespace":"com.acme","fields":[{"name":"street","type":"string"},{"name":"city","type":"com.acme.City"}]}
        """;
    private const string City = """{"type":"record","name":"City","namespace":"com.acme","fields":[{"name":"name","type":"string"}]}""";

    [Fact]
    public async Task AnAvroRecordUsingAnotherSubjectsTypesDecodes()
    {
        var registry = new Registry
        {
            ["schemas/ids/7"] = Answer(Order, refs: ("com.acme.Address", "address-value", 3)),
            ["subjects/address-value/versions/3"] = Answer(Address, refs: ("com.acme.City", "city-value", 1)),
            ["subjects/city-value/versions/1"] = Answer(City)
        };
        using var client = new SchemaRegistryClient("http://registry:8081", null, null, registry);

        var schema = (await client.GetAsync(7, CancellationToken.None))!;
        // id 42, street "Main", city "Oslo"
        byte[] body = [0, 0, 0, 0, 7, 0x54, 0x08, .. "Main"u8, 0x08, .. "Oslo"u8];
        var decoded = BodyDecoder.Decode(body, schema: schema)!;

        Assert.Equal(["com.acme.Address", "com.acme.City"], schema.Resolved.Select(item => item.Name));
        Assert.True(decoded.IsJson, decoded.Note);
        using var json = JsonDocument.Parse(decoded.Text);
        Assert.Equal(42, json.RootElement.GetProperty("id").GetInt64());
        Assert.Equal("Main", json.RootElement.GetProperty("addr").GetProperty("street").GetString());
        Assert.Equal("Oslo", json.RootElement.GetProperty("addr").GetProperty("city").GetProperty("name").GetString());
    }

    [Fact]
    public async Task AReferenceTheRegistryDoesNotHandOutIsNamed()
    {
        var registry = new Registry { ["schemas/ids/7"] = Answer(Order, refs: ("com.acme.Address", "address-value", 3)) };
        using var client = new SchemaRegistryClient("http://registry:8081", null, null, registry);

        var schema = (await client.GetAsync(7, CancellationToken.None))!;
        byte[] body = [0, 0, 0, 0, 7, 0x54, 0x08, .. "Main"u8];
        var decoded = BodyDecoder.Decode(body, schema: schema)!;

        Assert.Empty(schema.Resolved);
        Assert.Contains("(com.acme.Address) that the Schema Registry did not hand out", decoded.Note, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AProtoImportingAnotherSubjectsFileDecodesByName()
    {
        var registry = new Registry
        {
            ["schemas/ids/9"] = Answer("""
                syntax = "proto3";
                package acme;
                import "address.proto";
                message Order { int64 id = 1; Address addr = 2; }
                """, type: "PROTOBUF", refs: ("address.proto", "address-value", 1)),
            ["subjects/address-value/versions/1"] = Answer("""
                syntax = "proto3";
                package acme;
                message Address { string street = 1; }
                """, type: "PROTOBUF")
        };
        using var client = new SchemaRegistryClient("http://registry:8081", null, null, registry);

        var schema = (await client.GetAsync(9, CancellationToken.None))!;
        // magic, id 9, message index [0]; then id = 42, addr { street = "Main" }
        byte[] body = [0, 0, 0, 0, 9, 0x00, 0x08, 0x2A, 0x12, 0x06, 0x0A, 0x04, .. "Main"u8];
        var decoded = BodyDecoder.Decode(body, schema: schema)!;

        Assert.Contains("message acme.Order", decoded.Note, StringComparison.Ordinal);
        Assert.Contains("\"street\"", decoded.Text, StringComparison.Ordinal);
        Assert.Contains("Main", decoded.Text, StringComparison.Ordinal);
    }

    private static string Answer(string schema, string? type = null, params (string Name, string Subject, int Version)[] refs) =>
        JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["schema"] = schema,
            ["schemaType"] = type,
            ["references"] = refs.Select(item => new { name = item.Name, subject = item.Subject, version = item.Version }).ToArray()
        });

    private sealed class Registry : HttpMessageHandler, System.Collections.IEnumerable
    {
        private readonly Dictionary<string, string> _answers = new(StringComparer.Ordinal);
        public string this[string path] { set => _answers[path] = value; }
        public System.Collections.IEnumerator GetEnumerator() => _answers.GetEnumerator();

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            Task.FromResult(_answers.TryGetValue(request.RequestUri!.AbsolutePath.TrimStart('/'), out var answer)
                ? new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(answer) }
                : new HttpResponseMessage(HttpStatusCode.NotFound));
    }
}
