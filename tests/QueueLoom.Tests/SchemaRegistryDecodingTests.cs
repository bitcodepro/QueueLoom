using System.Net;
using System.Text;
using System.Text.Json;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

public sealed class SchemaRegistryDecodingTests
{
    private const string OrderSchema = """
        {"type":"record","name":"Order","fields":[{"name":"id","type":"long"},{"name":"customer","type":"string"},
         {"name":"note","type":["null","string"],"default":null}]}
        """;

    [Fact]
    public void AvroInWireFormat_IsDecodedWithTheRegistrySchema()
    {
        // id 42 (zig-zag 84), "ann", union branch 1 with "vip".
        byte[] payload = [84, 6, (byte)'a', (byte)'n', (byte)'n', 2, 6, (byte)'v', (byte)'i', (byte)'p'];
        var body = Framed(7, payload);

        var decoded = BodyDecoder.Decode(body, schema: new MessageSchema(7, MessageSchemaType.Avro, OrderSchema))!;

        Assert.Equal(["Schema Registry", "Avro"], decoded.Steps);
        using var json = JsonDocument.Parse(decoded.Text);
        Assert.Equal(42, json.RootElement.GetProperty("id").GetInt64());
        Assert.Equal("ann", json.RootElement.GetProperty("customer").GetString());
        Assert.Equal("vip", json.RootElement.GetProperty("note").GetString());
        Assert.Contains("Schema id 7, record Order", decoded.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void ProtobufInWireFormat_SkipsTheMessageIndexes()
    {
        // Message index list [0] is written as a single zero; then field 1 = 150.
        var body = Framed(3, [0, 0x08, 0x96, 0x01]);

        var decoded = BodyDecoder.Decode(body, schema: new MessageSchema(3, MessageSchemaType.Protobuf,
            "syntax = \"proto3\"; message Payment { int64 amount = 1; }"))!;

        Assert.Equal(["Schema Registry", "Protobuf"], decoded.Steps);
        Assert.Contains("150", decoded.Text, StringComparison.Ordinal);
        Assert.Contains("message Payment", decoded.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void JsonInWireFormat_IsRecognisedEvenWithoutARegistry()
    {
        var body = Framed(12, Encoding.UTF8.GetBytes("""{"orderId":42}"""));

        var decoded = BodyDecoder.Decode(body)!;

        Assert.Equal(["Schema Registry", "JSON"], decoded.Steps);
        Assert.Contains("\"orderId\": 42", decoded.Text, StringComparison.Ordinal);
        Assert.Contains("Schema id 12", decoded.Note, StringComparison.Ordinal);
    }

    [Fact]
    public void BinaryStartingWithZero_IsNotTakenForAFramedMessage()
    {
        Assert.False(BodyDecoder.TryReadSchemaId([0, 0, 0, 0, 0, 1], out _));
        Assert.True(BodyDecoder.TryReadSchemaId([0, 0, 0, 1, 0], out var id));
        Assert.Equal(256, id);
        var decoded = BodyDecoder.Decode(Framed(5, [0xFF, 0xFE, 0x00, 0x01]));
        Assert.DoesNotContain("Schema Registry", decoded?.Steps ?? [], StringComparer.Ordinal);
    }

    [Fact]
    public async Task RegistryClient_FetchesEachIdOnceAndSendsBasicAuth()
    {
        var handler = new RecordingHandler("""{"schemaType":"PROTOBUF","schema":"message A {}"}""");
        using var client = new SchemaRegistryClient("http://registry:8081/", "key", "secret", handler);

        var first = await client.GetAsync(9, CancellationToken.None);
        var second = await client.GetAsync(9, CancellationToken.None);

        Assert.Equal(new MessageSchema(9, MessageSchemaType.Protobuf, "message A {}"), first);
        Assert.Same(first, second);
        Assert.Equal(1, handler.Calls);
        Assert.Equal("http://registry:8081/schemas/ids/9", handler.LastUri);
        Assert.Equal("Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes("key:secret")), handler.LastAuthorization);
    }

    [Fact]
    public async Task RegistryClient_ReturnsNullWhenTheIdIsUnknown()
    {
        using var client = new SchemaRegistryClient("http://registry:8081", null, null, new RecordingHandler(null));

        Assert.Null(await client.GetAsync(1, CancellationToken.None));
    }

    private static byte[] Framed(int id, byte[] payload) =>
        [0, (byte)(id >> 24), (byte)(id >> 16), (byte)(id >> 8), (byte)id, .. payload];

    private sealed class RecordingHandler(string? json) : HttpMessageHandler
    {
        public int Calls { get; private set; }
        public string? LastUri { get; private set; }
        public string? LastAuthorization { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Calls++;
            LastUri = request.RequestUri?.ToString();
            LastAuthorization = request.Headers.Authorization?.ToString();
            return Task.FromResult(json is null
                ? new HttpResponseMessage(HttpStatusCode.NotFound)
                : new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(json, Encoding.UTF8, "application/json") });
        }
    }
}

public sealed class LogReadStartTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 30, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void RelativeTimes_CountBackFromNow()
    {
        Assert.Equal(Now.AddMinutes(-30), QueueLoom.App.ViewModels.MainWindowViewModel.ParseLogStart(BrowseStartKind.FromTime, "30m", Now).Time);
        Assert.Equal(Now.AddHours(-2), QueueLoom.App.ViewModels.MainWindowViewModel.ParseLogStart(BrowseStartKind.FromTime, "2h", Now).Time);
        Assert.Equal(Now.AddDays(-1), QueueLoom.App.ViewModels.MainWindowViewModel.ParseLogStart(BrowseStartKind.FromTime, "1d", Now).Time);
    }

    [Fact]
    public void Offsets_MayNameAPartition()
    {
        Assert.Equal(new BrowseStart(BrowseStartKind.FromOffset, Offset: 1500),
            QueueLoom.App.ViewModels.MainWindowViewModel.ParseLogStart(BrowseStartKind.FromOffset, "1500", Now));
        Assert.Equal(new BrowseStart(BrowseStartKind.FromOffset, Offset: 1500, Partition: 2),
            QueueLoom.App.ViewModels.MainWindowViewModel.ParseLogStart(BrowseStartKind.FromOffset, " 2:1500 ", Now));
        Assert.Throws<InvalidOperationException>(() =>
            QueueLoom.App.ViewModels.MainWindowViewModel.ParseLogStart(BrowseStartKind.FromOffset, "-1", Now));
        Assert.Throws<InvalidOperationException>(() =>
            QueueLoom.App.ViewModels.MainWindowViewModel.ParseLogStart(BrowseStartKind.FromTime, "yesterday-ish", Now));
    }
}
