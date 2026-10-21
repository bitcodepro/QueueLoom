using System.Net;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

// A 200 answer whose JSON has no string "schema" (a wrong address that reaches another JSON API, a proxy's status page)
// was cached as "this id has no schema" for good: that id was never asked again, even after the address was fixed, and
// nothing said why messages stayed undecoded. It is now handled like any other answer that is not a schema: reported
// once per outage and asked again after the backoff.
public sealed class SchemaRegistryAnswerWithoutSchemaTests
{
    [Theory]
    [InlineData("""{"status":"ok"}""")]
    [InlineData("""{"schema":42}""")]
    [InlineData("""["schema"]""")]
    public async Task AnAnswerWithoutASchemaIsReportedAndAskedAgainLater(string answer)
    {
        var time = new ManualTime();
        var handler = new Handler(answer);
        using var client = new SchemaRegistryClient("http://registry:8081", null, null, handler, time);

        Assert.Null(await client.GetAsync(1, CancellationToken.None));
        var problem = client.TakeOutage();
        Assert.NotNull(problem);
        Assert.Contains("was not a schema", problem, StringComparison.Ordinal);

        time.Advance(SchemaRegistryClient.FailureBackoff + TimeSpan.FromSeconds(1));
        handler.Answer = """{"schemaType":"PROTOBUF","schema":"message A {}"}""";
        Assert.NotNull(await client.GetAsync(1, CancellationToken.None));
        Assert.Equal(2, handler.Requests);
    }

    private sealed class Handler(string answer) : HttpMessageHandler
    {
        public string Answer { get; set; } = answer;
        public int Requests { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Requests++;
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(Answer) });
        }
    }

    private sealed class ManualTime : TimeProvider
    {
        private DateTimeOffset _now = DateTimeOffset.UtcNow;
        public override DateTimeOffset GetUtcNow() => _now;
        public void Advance(TimeSpan by) => _now += by;
    }
}
