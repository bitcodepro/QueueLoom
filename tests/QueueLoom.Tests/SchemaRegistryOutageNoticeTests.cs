using System.Net;
using System.Reflection;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Kafka;
using QueueLoom.Infrastructure.Messaging;

namespace QueueLoom.Tests;

// After a Schema Registry failure, lookups are skipped for a while so an outage costs one request per browse, not one
// per record. Messages were then shown undecoded with no word why. The client now records the reason once per outage,
// and a Kafka browse that met it reports a notice the app shows in its activity list.
public sealed class SchemaRegistryOutageNoticeTests
{
    private const BindingFlags Any = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    [Theory]
    [InlineData("503", "503")]
    [InlineData("unreachable", "could not be reached")]
    public async Task AnOutageIsReportedOncePerOutage(string failure, string expected)
    {
        var time = new ManualTime();
        var handler = new FailingHandler(failure);
        using var client = new SchemaRegistryClient("http://registry:8081", null, null, handler, time);

        Assert.Null(await client.GetAsync(1, CancellationToken.None));
        var problem = client.TakeOutage();
        Assert.NotNull(problem);
        Assert.Contains(expected, problem, StringComparison.Ordinal);
        Assert.Null(await client.GetAsync(2, CancellationToken.None)); // skipped during the backoff
        Assert.Null(client.TakeOutage());                                // already reported

        time.Advance(SchemaRegistryClient.FailureBackoff + TimeSpan.FromSeconds(1));
        handler.Healthy = true;
        Assert.NotNull(await client.GetAsync(3, CancellationToken.None));
        Assert.Null(client.TakeOutage());

        handler.Healthy = false;
        Assert.Null(await client.GetAsync(4, CancellationToken.None)); // a new outage after recovery is reported again
        Assert.NotNull(client.TakeOutage());
    }

    [Fact]
    public async Task AKafkaBrowseThatMetAnOutageRaisesOneNotice()
    {
        await using var owner = new KafkaWorkspace(new DeepAuditCloudTests.EmptyVault());
        var fixture = new PubSubKafkaStabilityTests.Fixture { Body = [0, 0, 0, 0, 7, (byte)'x'] };
        owner.ConsumerFactory = fixture.Create;
        typeof(KafkaWorkspace).GetField("_schemaRegistry", Any)!.SetValue(owner,
            new SchemaRegistryClient("http://registry.test", null, null, new FailingHandler("503")));
        var notices = new List<WorkspaceNotice>();
        ((IWorkspaceNoticeSource)owner).Notice += (_, notice) => notices.Add(notice);
        var type = typeof(KafkaWorkspace).GetNestedType("KafkaChannel", BindingFlags.NonPublic)!;
        var channel = (ILeasedMessageChannel)Activator.CreateInstance(type, Any, null,
            [owner, new KafkaTopicInfo("isolated", [0], 3), ServiceBusEntityReference.Queue("isolated"), ServiceBusSubQueue.Active, null], null)!;
        try
        {
            var page = await channel.ReceiveAsync(10, CancellationToken.None);

            Assert.Equal(3, page.Count);
            var notice = Assert.Single(notices);
            Assert.Contains("Schema Registry", notice.Title, StringComparison.Ordinal);
            Assert.Contains("503", notice.Details, StringComparison.Ordinal);
        }
        finally
        {
            ((IDisposable)channel).Dispose();
        }
    }

    private sealed class FailingHandler(string failure) : HttpMessageHandler
    {
        public bool Healthy { get; set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (Healthy)
            {
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("""{"schemaType":"PROTOBUF","schema":"message A {}"}""")
                });
            }
            return failure == "503"
                ? Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable))
                : Task.FromException<HttpResponseMessage>(new HttpRequestException("No such host is known."));
        }
    }

    private sealed class ManualTime : TimeProvider
    {
        private DateTimeOffset _now = DateTimeOffset.UtcNow;
        public override DateTimeOffset GetUtcNow() => _now;
        public void Advance(TimeSpan by) => _now += by;
    }
}
