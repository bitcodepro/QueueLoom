using System.Net;
using System.Text.Json;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

// Review of the reference resolution: a passing failure of a referenced schema (503, a dropped connection) was cached
// for good as an incomplete schema, so it was never fetched again after the registry recovered; and the bound counted
// successful fetches, not requests, so 40 missing references meant 40 requests and a wide graph queued without limit.
public sealed class SchemaReferencesBoundsTests
{
    private const string Order = """
        {"type":"record","name":"Order","namespace":"com.acme","fields":[{"name":"addr","type":"com.acme.Address"}]}
        """;
    private const string Address = """{"type":"record","name":"Address","namespace":"com.acme","fields":[{"name":"street","type":"string"}]}""";

    [Fact]
    public async Task AReferenceThatFailedForNowIsFetchedAgainAfterTheBackoff()
    {
        var time = new ManualTime();
        var registry = new Registry();
        registry.Answer("schemas/ids/7", () => (HttpStatusCode.OK, Schema(Order, ("com.acme.Address", "address-value", 3))));
        var down = true;
        registry.Answer("subjects/address-value/versions/3", () => down ? (HttpStatusCode.ServiceUnavailable, "") : (HttpStatusCode.OK, Schema(Address)));
        using var client = new SchemaRegistryClient("http://registry:8081", null, null, registry, time);

        var first = (await client.GetAsync(7, CancellationToken.None))!;
        down = false;
        var duringBackoff = (await client.GetAsync(7, CancellationToken.None))!;
        time.Advance(SchemaRegistryClient.FailureBackoff + TimeSpan.FromSeconds(1));
        var afterBackoff = (await client.GetAsync(7, CancellationToken.None))!;

        Assert.Empty(first.Resolved);
        Assert.Equal(["com.acme.Address"], first.Unresolved);
        Assert.NotNull(client.TakeOutage());
        Assert.Same(first, duringBackoff);
        Assert.Equal(["com.acme.Address"], afterBackoff.Resolved.Select(item => item.Name));
        Assert.Empty(afterBackoff.Unresolved);
    }

    [Fact]
    public async Task AMissingReferenceIsAnsweredForGood()
    {
        var registry = new Registry();
        registry.Answer("schemas/ids/7", () => (HttpStatusCode.OK, Schema(Order, ("com.acme.Address", "address-value", 3))));
        using var client = new SchemaRegistryClient("http://registry:8081", null, null, registry);

        await client.GetAsync(7, CancellationToken.None);
        await client.GetAsync(7, CancellationToken.None);

        Assert.Equal(1, registry.Requests("subjects/address-value/versions/3"));
        Assert.Null(client.TakeOutage());
    }

    [Fact]
    public async Task ManyMissingReferencesCostAtMost32Requests()
    {
        var registry = new Registry();
        var references = Enumerable.Range(1, 40).Select(index => ($"com.acme.T{index}", $"t{index}-value", 1)).ToArray();
        registry.Answer("schemas/ids/7", () => (HttpStatusCode.OK, Schema(Order, references)));
        using var client = new SchemaRegistryClient("http://registry:8081", null, null, registry);

        var schema = (await client.GetAsync(7, CancellationToken.None))!;

        Assert.Equal(32, registry.SubjectRequests);
        Assert.Equal(40, schema.Unresolved.Count);
    }

    [Fact]
    public async Task AWideGraphIsBoundedByRequestsNotBySuccesses()
    {
        var registry = new Registry();
        registry.Answer("schemas/ids/7", () => (HttpStatusCode.OK,
            Schema(Order, Enumerable.Range(1, 10).Select(index => ($"com.acme.A{index}", $"a{index}-value", 1)).ToArray())));
        for (var parent = 1; parent <= 10; parent++)
        {
            var children = Enumerable.Range(1, 10).Select(child => ($"com.acme.A{parent}B{child}", $"a{parent}b{child}-value", 1)).ToArray();
            registry.Answer($"subjects/a{parent}-value/versions/1", () => (HttpStatusCode.OK, Schema(Address, children)));
            foreach (var (_, subject, _) in children)
                registry.Answer($"subjects/{subject}/versions/1", () => (HttpStatusCode.OK, Schema(Address)));
        }
        using var client = new SchemaRegistryClient("http://registry:8081", null, null, registry);

        var schema = (await client.GetAsync(7, CancellationToken.None))!;

        Assert.Equal(32, registry.SubjectRequests);
        Assert.Equal(32, schema.Resolved.Count);
        Assert.NotEmpty(schema.Unresolved);
    }

    [Fact]
    public async Task AMissingReferenceOfAReferenceIsNamedInTheNote()
    {
        var registry = new Registry();
        registry.Answer("schemas/ids/7", () => (HttpStatusCode.OK, Schema(Order, ("com.acme.Address", "address-value", 3))));
        registry.Answer("subjects/address-value/versions/3", () => (HttpStatusCode.OK, Schema("""
            {"type":"record","name":"Address","namespace":"com.acme","fields":[{"name":"city","type":"com.acme.City"}]}
            """, ("com.acme.City", "city-value", 1))));
        using var client = new SchemaRegistryClient("http://registry:8081", null, null, registry);

        var schema = (await client.GetAsync(7, CancellationToken.None))!;
        byte[] body = [0, 0, 0, 0, 7, 0x08, .. "Oslo"u8];
        var decoded = BodyDecoder.Decode(body, schema: schema)!;

        Assert.Equal(["com.acme.City"], schema.Unresolved);
        Assert.Contains("(com.acme.City) that the Schema Registry did not hand out", decoded.Note, StringComparison.Ordinal);
    }

    private static string Schema(string schema, params (string Name, string Subject, int Version)[] refs) =>
        JsonSerializer.Serialize(new
        {
            schema,
            references = refs.Select(item => new { name = item.Name, subject = item.Subject, version = item.Version }).ToArray()
        });

    private sealed class Registry : HttpMessageHandler
    {
        private readonly Dictionary<string, Func<(HttpStatusCode Status, string Body)>> _answers = new(StringComparer.Ordinal);
        private readonly Dictionary<string, int> _requests = new(StringComparer.Ordinal);
        public void Answer(string path, Func<(HttpStatusCode, string)> answer) => _answers[path] = answer;
        public int Requests(string path) { lock (_requests) return _requests.GetValueOrDefault(path); }
        public int SubjectRequests { get { lock (_requests) return _requests.Where(pair => pair.Key.StartsWith("subjects/", StringComparison.Ordinal)).Sum(pair => pair.Value); } }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var path = request.RequestUri!.AbsolutePath.TrimStart('/');
            lock (_requests) _requests[path] = _requests.GetValueOrDefault(path) + 1;
            if (!_answers.TryGetValue(path, out var answer)) return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound));
            var (status, body) = answer();
            return Task.FromResult(new HttpResponseMessage(status) { Content = new StringContent(body) });
        }
    }

    private sealed class ManualTime : TimeProvider
    {
        private DateTimeOffset _now = DateTimeOffset.UtcNow;
        public override DateTimeOffset GetUtcNow() => _now;
        public void Advance(TimeSpan by) => _now += by;
    }
}
