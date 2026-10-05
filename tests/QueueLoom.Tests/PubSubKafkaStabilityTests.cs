using System.Reflection;
using Confluent.Kafka;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Google;
using QueueLoom.Infrastructure.Kafka;
using QueueLoom.Infrastructure.Messaging;

namespace QueueLoom.Tests;

public sealed class PubSubKafkaStabilityTests
{
    private const BindingFlags Any = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    // A consumer whose Close fails (broker gone) must still be disposed and dropped, or every later operation
    // on the workspace fails while closing the previous channel, and the native consumer leaks.
    [Fact]
    public async Task Kafka_ChannelDisposeReleasesConsumerEvenWhenCloseFails()
    {
        await using var owner = new KafkaWorkspace(new EmptyVault());
        var fixture = new Fixture { FailClose = true };
        var channel = NewChannel(owner, out var type);
        type.GetField("_consumer", Any)!.SetValue(channel, fixture.Create());

        try { ((IDisposable)channel).Dispose(); } catch (KafkaException) { }

        Assert.True(fixture.Disposed);
        Assert.Null(type.GetField("_consumer", Any)!.GetValue(channel));
        ((IDisposable)channel).Dispose(); // a second dispose must not throw again
    }

    // A failure while positioning the consumer (watermarks unavailable) must not leave a half-assigned consumer
    // that later reads report as an empty topic.
    [Fact]
    public async Task Kafka_FailedOpenIsRetriedInsteadOfReportingAnEmptyTopic()
    {
        await using var owner = new KafkaWorkspace(new EmptyVault());
        var fixture = new Fixture { FailWatermarksOnce = true };
        var channel = NewChannel(owner, out var type);
        owner.ConsumerFactory = fixture.Create;
        try
        {
            await Assert.ThrowsAsync<KafkaException>(() => channel.ReceiveAsync(10, CancellationToken.None));
            Assert.True(fixture.Disposed);
            var page = await channel.ReceiveAsync(10, CancellationToken.None);
            Assert.Equal(3, page.Count);
        }
        finally
        {
            ((IDisposable)channel).Dispose();
        }
    }

    // Acknowledging in chunks: when a later chunk fails, the chunks Pub/Sub already acknowledged are gone and must
    // not be reported as still there.
    [Fact]
    public async Task PubSub_PartialAcknowledgeReportsOnlyTheUnacknowledgedChunks()
    {
        await using var workspace = new GooglePubSubWorkspace(new EmptyVault());
        var subscriber = new AckSubscriber();
        typeof(GooglePubSubWorkspace).GetField("_subscriber", Any)!.SetValue(workspace, subscriber);
        var type = typeof(GooglePubSubWorkspace).GetNestedType("PubSubChannel", BindingFlags.NonPublic)!;
        var source = ServiceBusEntityReference.Subscription("t", "s");
        var channel = (ILeasedMessageChannel)Activator.CreateInstance(type, Any, null,
            [workspace, source, ServiceBusSubQueue.Active, new SubscriptionName("p", "s"), null], null)!;
        var messages = Enumerable.Range(0, 1_500).Select(i => new LeasedMessage(
            GooglePubSubWorkspace.ToBrowsedMessage(new ReceivedMessage { AckId = $"a{i}", Message = new PubsubMessage { MessageId = $"m{i}" } },
                source, ServiceBusSubQueue.Active), $"a{i}", true)).ToList();

        var failed = await channel.SettleAsync(messages, CancellationToken.None);

        Assert.Equal(500, failed.Count);
        Assert.All(failed, message => Assert.DoesNotContain(message.LeaseHandle, subscriber.Acked));
    }

    // Cancelled while the schemas of a page are being looked up: the records already read are handed over (without
    // schemas) instead of being dropped, so the channel, which has moved past them, does not lose them.
    [Fact]
    public async Task Kafka_CancellingDuringSchemaLookupKeepsTheRecordsRead()
    {
        await using var owner = new KafkaWorkspace(new EmptyVault());
        var fixture = new Fixture { Body = [0, 0, 0, 0, 7, (byte)'x'] };
        owner.ConsumerFactory = fixture.Create;
        using var cancellation = new CancellationTokenSource();
        var handler = new CancellingHandler(cancellation);
        typeof(KafkaWorkspace).GetField("_schemaRegistry", Any)!.SetValue(owner,
            new SchemaRegistryClient("http://registry.test", null, null, handler));
        var channel = NewChannel(owner, out _);
        try
        {
            var page = await channel.ReceiveAsync(10, cancellation.Token);

            Assert.Equal(3, page.Count);
            Assert.All(page, message => Assert.Null(message.Message.Schema));
            Assert.Equal(1, handler.Calls);
        }
        finally
        {
            ((IDisposable)channel).Dispose();
        }
    }

    private sealed class CancellingHandler(CancellationTokenSource cancellation) : HttpMessageHandler
    {
        public int Calls { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Calls++;
            await cancellation.CancelAsync();
            cancellationToken.ThrowIfCancellationRequested();
            return new HttpResponseMessage(System.Net.HttpStatusCode.NotFound);
        }
    }

    private static ILeasedMessageChannel NewChannel(KafkaWorkspace owner, out Type type)
    {
        type = typeof(KafkaWorkspace).GetNestedType("KafkaChannel", BindingFlags.NonPublic)!;
        var topic = new KafkaTopicInfo("isolated", [0], 3);
        return (ILeasedMessageChannel)Activator.CreateInstance(type, Any, null,
            [owner, topic, ServiceBusEntityReference.Queue("isolated"), ServiceBusSubQueue.Active, null], null)!;
    }

    public class ConsumerProxy : DispatchProxy
    {
        internal Fixture Fixture { get; set; } = null!;
        protected override object? Invoke(MethodInfo? method, object?[]? args) => Fixture.Invoke(method!.Name, args!);
    }

    internal sealed class Fixture
    {
        public bool FailClose { get; init; }
        public byte[] Body { get; init; } = "body"u8.ToArray();
        public bool FailWatermarksOnce { get; set; }
        public bool Disposed { get; private set; }
        private long _position = -1;

        public IConsumer<byte[]?, byte[]?> Create()
        {
            Disposed = false;
            _position = -1;
            var consumer = DispatchProxy.Create<IConsumer<byte[]?, byte[]?>, ConsumerProxy>();
            ((ConsumerProxy)(object)consumer).Fixture = this;
            return consumer;
        }

        public object? Invoke(string method, object?[] args)
        {
            switch (method)
            {
                case "Close" when FailClose:
                    throw new KafkaException(ErrorCode.Local_Transport);
                case "Dispose":
                    Disposed = true;
                    return null;
                case "QueryWatermarkOffsets":
                    if (FailWatermarksOnce)
                    {
                        FailWatermarksOnce = false;
                        throw new KafkaException(ErrorCode.Local_TimedOut);
                    }
                    return new WatermarkOffsets(0, 3);
                case "Assign":
                    foreach (var assignment in (IEnumerable<TopicPartitionOffset>)args[0]!) _position = assignment.Offset.Value;
                    return null;
                case "Consume":
                    if (_position < 0 || _position >= 3) return null;
                    var offset = _position++;
                    return new ConsumeResult<byte[]?, byte[]?>
                    {
                        Topic = "isolated", Partition = 0, Offset = offset,
                        Message = new() { Value = Body, Timestamp = new Timestamp(DateTime.UnixEpoch), Headers = new Headers() }
                    };
                default:
                    return null;
            }
        }
    }

    private sealed class AckSubscriber : SubscriberServiceApiClient
    {
        private int _calls;
        public HashSet<string> Acked { get; } = new(StringComparer.Ordinal);

        public override Task AcknowledgeAsync(AcknowledgeRequest request, CallSettings? callSettings = null)
        {
            if (++_calls > 1)
            {
                throw new RpcException(new Status(StatusCode.Unavailable, "down"));
            }
            Acked.UnionWith(request.AckIds);
            return Task.CompletedTask;
        }
    }

    private sealed class EmptyVault : ISecretVault
    {
        public ValueTask StoreAsync(ProfileSecretKey key, string value, CancellationToken token = default) => ValueTask.CompletedTask;
        public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult<string?>(null);
        public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
        public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
    }
}
