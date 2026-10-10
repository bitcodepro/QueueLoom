using System.Reflection;
using Confluent.Kafka;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

// Runs librdkafka's in-process mock broker: a real Kafka protocol server whose requests the tests can fail. It does not
// deduplicate retried sequence numbers, so the lost-acknowledgement case runs against a real broker instead
// (QueueLoom.IntegrationTests.KafkaLostAcknowledgementTests).
public sealed class KafkaIdempotentProducerTests
{
    private const string Topic = "orders";

    // A cluster that refuses an idempotent producer ID (Kafka before 2.8 without IDEMPOTENT_WRITE): the send reports a
    // clear error, does not quietly fall back to a non-idempotent send, and writes nothing.
    [Fact]
    public async Task ARefusedIdempotentProducerIsReportedAndNothingIsSentBehindTheUsersBack()
    {
        using var cluster = new KafkaMockCluster();
        cluster.CreateTopic(Topic);
        cluster.FailRequests(KafkaMockCluster.InitProducerIdApi, KafkaMockCluster.ClusterAuthorizationFailed, 100);
        await using var workspace = await ConnectAsync(cluster, compatibilityMode: false, producer => producer.MessageTimeoutMs = 5_000);

        var error = await Assert.ThrowsAnyAsync<Exception>(() => SendAsync(workspace, "order-1").WaitAsync(TimeSpan.FromSeconds(30)));

        // Reported as unknown, never as a proven refusal: "purged in queue" can also be a written record waiting for a retry.
        Assert.IsType<InvalidOperationException>(error);
        Assert.Contains("unknown", error.Message, StringComparison.Ordinal);
        Assert.Contains("Cluster authorization failed", error.Message, StringComparison.Ordinal);
        Assert.Contains("IDEMPOTENT_WRITE", error.Message, StringComparison.Ordinal);
        Assert.Contains("compatibility mode", error.Message, StringComparison.Ordinal);
        await Task.Delay(TimeSpan.FromSeconds(1));
        Assert.DoesNotContain("order-1", ReadAll(cluster));
    }

    // A move whose send times out: the outcome is unknown, so the original dead letter is kept and nothing is deleted.
    [Fact]
    public async Task AMoveWhoseSendOutcomeIsUnknownKeepsTheOriginal()
    {
        using var cluster = new KafkaMockCluster();
        cluster.CreateTopic(Topic);
        await using var kafka = await ConnectAsync(cluster, compatibilityMode: false, producer => producer.MessageTimeoutMs = 2_000);
        await SendAsync(kafka, "warm-up");
        // Every produce attempt is answered "request timed out", so the client retries until the message times out.
        cluster.FailRequests(KafkaMockCluster.ProduceApi, KafkaMockCluster.RequestTimedOut, 1_000);
        var workspace = DispatchProxy.Create<IServiceBusWorkspace, DeleteRecorder>();
        ((DeleteRecorder)(object)workspace).Inner = kafka;
        var original = new BrowsedMessage(ServiceBusEntityReference.Queue(Topic + ".DLT"), ServiceBusSubQueue.DeadLetter, 7,
            "{}"u8.ToArray(), new EditableMessageProperties(MessageId: "m-7"));
        var item = new ResendItem(original, ServiceBusEntityReference.Queue(Topic), original.CreateDraft());

        var result = await DeadLetterResender.ResendAsync(workspace, [item], ResendMode.Move).WaitAsync(TimeSpan.FromSeconds(30));

        var outcome = Assert.Single(result.Items);
        Assert.Equal(ResendOutcome.Failed, outcome.Outcome);
        Assert.Equal(0, ((DeleteRecorder)(object)workspace).Deletes);
    }

    // A record whose acknowledgement was lost waits in librdkafka's retry queue; a fatal producer error then purges it
    // with "purged in queue", and ProduceAsync drops its "possibly persisted" status. That must stay unknown.
    [Fact]
    public async Task AFatalPurgeOfAQueuedRecordIsNotAProvenRefusal()
    {
        var error = await FatalPurgeOutcomeAsync();

        Assert.IsNotType<DeliveryRejectedException>(error);
        Assert.Contains("unknown", error.Message, StringComparison.Ordinal);
        Assert.Contains("Cluster authorization failed", error.Message, StringComparison.Ordinal);
    }

    /// <summary>What the workspace reports for a record purged in queue after the producer stopped.</summary>
    internal static async Task<Exception> FatalPurgeOutcomeAsync()
    {
        await using var workspace = new KafkaWorkspace(new NoSecrets());
        var producer = DispatchProxy.Create<IProducer<byte[]?, byte[]?>, BrokerOutcomeTests.FailingProducer>();
        ((BrokerOutcomeTests.FailingProducer)(object)producer).Code = ErrorCode.Local_PurgeQueue;
        typeof(KafkaWorkspace).GetField("_producer", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, producer);
        typeof(KafkaWorkspace).GetField("_producerFatalError", BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(workspace, new Error(ErrorCode.ClusterAuthorizationFailed, "Broker: Cluster authorization failed", true));
        return await Assert.ThrowsAnyAsync<Exception>(() => SendCoreAsync(workspace, ServiceBusEntityReference.Queue(Topic),
            new MessageDraft(new EditableMessageBody("x", MessageBodyFormat.Text), EditableMessageProperties.Empty)));
    }

    private static async Task<KafkaWorkspace> ConnectAsync(KafkaMockCluster cluster, bool compatibilityMode, Action<ProducerConfig> configure)
    {
        var workspace = new KafkaWorkspace(new NoSecrets()) { ConfigureProducer = configure };
        var profile = ServiceBusProfile.CreateNew("Mock Kafka", EnvironmentKind.Development,
                new AuthenticationSettings(AuthenticationKind.KafkaNone), accessMode: ProfileAccessMode.ReadWrite) with
        {
            Provider = MessagingProvider.Kafka,
            Kafka = new KafkaSettings(cluster.BootstrapServers, NonIdempotentProducer: compatibilityMode)
        };
        await workspace.ConnectAsync(profile);
        return workspace;
    }

    private static Task SendAsync(KafkaWorkspace workspace, string body) =>
        SendCoreAsync(workspace, ServiceBusEntityReference.Queue(Topic),
            new MessageDraft(new EditableMessageBody(body, MessageBodyFormat.Text), EditableMessageProperties.Empty));

    // The workspace's own send path, minus the topology read before it: that read lists consumer groups, which the mock
    // broker does not support, and destroying an admin client after that failed request crashes librdkafka.
    private static Task SendCoreAsync(KafkaWorkspace workspace, ServiceBusEntityReference destination, MessageDraft draft) =>
        (Task)typeof(KafkaWorkspace).GetMethod("SendCoreAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(workspace, [null, destination, draft, CancellationToken.None])!;

    private static List<string> ReadAll(KafkaMockCluster cluster)
    {
        using var consumer = new ConsumerBuilder<Ignore, string>(new ConsumerConfig
        {
            BootstrapServers = cluster.BootstrapServers, GroupId = Guid.NewGuid().ToString("N"),
            AutoOffsetReset = AutoOffsetReset.Earliest, EnablePartitionEof = true, EnableAutoCommit = false
        }).Build();
        consumer.Assign(new TopicPartitionOffset(Topic, 0, Offset.Beginning));
        var values = new List<string>();
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(1));
            if (result is null) continue;
            if (result.IsPartitionEOF) break;
            values.Add(result.Message.Value);
        }
        return values;
    }

    public class DeleteRecorder : DispatchProxy
    {
        public IServiceBusWorkspace Inner { get; set; } = null!;
        public int Deletes { get; private set; }

        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            if (method!.Name == nameof(IServiceBusWorkspace.DeleteDeadLetterMessagesAsync)) Deletes++;
            if (method.Name == nameof(IServiceBusWorkspace.SendMessageAsync))
            {
                var request = (SendMessageRequest)args![0]!;
                return SendCoreAsync((KafkaWorkspace)Inner, request.Destination, request.Message);
            }
            try { return method.Invoke(Inner, args); }
            catch (TargetInvocationException exception) when (exception.InnerException is not null)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(exception.InnerException);
                throw;
            }
        }
    }

    private sealed class NoSecrets : ISecretVault
    {
        public ValueTask StoreAsync(ProfileSecretKey key, string value, CancellationToken token = default) => ValueTask.CompletedTask;
        public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult<string?>(null);
        public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
        public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken token = default) => ValueTask.FromResult(false);
    }
}
