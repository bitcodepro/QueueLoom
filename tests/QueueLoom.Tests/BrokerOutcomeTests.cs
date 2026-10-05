using System.Reflection;
using Confluent.Kafka;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Kafka;

namespace QueueLoom.Tests;

public sealed class BrokerOutcomeTests
{
    // librdkafka tells whether a failed record may still have been written. A timeout after the request left
    // (possibly persisted) is uncertain, not "did not accept"; only "not persisted" is a refusal the app may treat as
    // certain (DeliveryRejectedException), and "persisted" says the message was stored.
    [Theory]
    [InlineData(PersistenceStatus.PossiblyPersisted, "unknown", false)]
    [InlineData(PersistenceStatus.NotPersisted, "did not accept", true)]
    [InlineData(PersistenceStatus.Persisted, "stored the message", false)]
    public async Task KafkaSendFailuresSayWhetherTheRecordMayHaveBeenWritten(PersistenceStatus status, string expected, bool rejected)
    {
        await using var workspace = new KafkaWorkspace(new EmptyVault());
        var producer = DispatchProxy.Create<IProducer<byte[]?, byte[]?>, FailingProducer>();
        ((FailingProducer)(object)producer).Status = status;
        typeof(KafkaWorkspace).GetField("_producer", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(workspace, producer);
        var send = typeof(KafkaWorkspace).GetMethod("SendCoreAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var draft = new MessageDraft(new EditableMessageBody("x", MessageBodyFormat.Text), EditableMessageProperties.Empty);

        var error = await Assert.ThrowsAnyAsync<Exception>(() =>
            (Task)send.Invoke(workspace, [null, ServiceBusEntityReference.Queue("orders"), draft, CancellationToken.None])!);

        Assert.Contains(expected, error.Message, StringComparison.Ordinal);
        Assert.Equal(rejected, error is DeliveryRejectedException);
    }

    public class FailingProducer : DispatchProxy
    {
        public PersistenceStatus Status { get; set; }

        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            if (method!.Name == "ProduceAsync")
            {
                return Task.FromException<DeliveryResult<byte[]?, byte[]?>>(new ProduceException<byte[]?, byte[]?>(
                    new Error(ErrorCode.Local_MsgTimedOut, "Message timed out"),
                    new DeliveryResult<byte[]?, byte[]?> { Status = Status }));
            }
            return method.ReturnType.IsValueType && method.ReturnType != typeof(void) ? Activator.CreateInstance(method.ReturnType) : null;
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

public sealed class AzurePurgeReleaseTests
{
    private const BindingFlags Any = BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public;

    // Cancelled while a received batch is being backed up: nothing of it is deleted, and its messages are released
    // at once (abandoned) instead of staying locked, unseen by everyone, until their lock expires.
    [Fact]
    public async Task ACancelledPurgeBatchIsReleasedNotLeftLocked()
    {
        using var cancellation = new CancellationTokenSource();
        var receiver = new ScriptedReceiver(3) { OnReceive = cancellation.Cancel };
        var result = await PurgeAsync(receiver, cancellation.Token);

        Assert.Equal(0, result.DeletedCount);
        Assert.Equal(3, receiver.Abandoned);
        Assert.Equal(0, receiver.Completed);
    }

    // One message of a batch cannot be deleted: it is released right away; the deleted ones are not touched again.
    [Fact]
    public async Task AMessageThatCouldNotBeDeletedIsReleased()
    {
        var receiver = new ScriptedReceiver(3) { FailCompleteOf = 1 };
        var result = await PurgeAsync(receiver, CancellationToken.None);

        Assert.Equal(2, result.DeletedCount);
        Assert.Equal(1, receiver.Abandoned);
    }

    private static async Task<DeadLetterPurgeSourceResult> PurgeAsync(ScriptedReceiver receiver, CancellationToken token)
    {
        using var directory = new QueueLoom.Tests.Infrastructure.TemporaryDirectory();
        await using var workspace = new QueueLoom.Infrastructure.Azure.AzureServiceBusWorkspace(new DeepAuditCloudTests.EmptyVault());
        typeof(QueueLoom.Infrastructure.Azure.AzureServiceBusWorkspace).GetField("_client", Any)!.SetValue(workspace, new ScriptedClient(receiver));
        var profile = ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Development, new(AuthenticationKind.ConnectionString));
        var session = await new QueueLoom.Infrastructure.Persistence.DeadLetterJsonBackupStore(
            QueueLoom.Infrastructure.Persistence.QueueLoomPaths.ForRoot(directory.Path)).CreateSessionAsync(profile, DateTimeOffset.UtcNow, default);
        var purge = typeof(QueueLoom.Infrastructure.Azure.AzureServiceBusWorkspace).GetMethod("PurgeSubQueueAsync", Any)!;
        return await (Task<DeadLetterPurgeSourceResult>)purge.Invoke(workspace,
            [ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 10, 10, session, 1, 1, null, token])!;
    }

    private sealed class ScriptedClient(ScriptedReceiver receiver) : Azure.Messaging.ServiceBus.ServiceBusClient
    {
        public override Azure.Messaging.ServiceBus.ServiceBusReceiver CreateReceiver(string queueName, Azure.Messaging.ServiceBus.ServiceBusReceiverOptions options) => receiver;
    }

    private sealed class ScriptedReceiver(int count) : Azure.Messaging.ServiceBus.ServiceBusReceiver
    {
        private bool _delivered;
        public Action? OnReceive { get; init; }
        public int FailCompleteOf { get; init; } = -1;
        public int Completed { get; private set; }
        public int Abandoned { get; private set; }

        public override Task<IReadOnlyList<Azure.Messaging.ServiceBus.ServiceBusReceivedMessage>> ReceiveMessagesAsync(
            int maxMessages, TimeSpan? maxWaitTime = null, CancellationToken cancellationToken = default)
        {
            if (_delivered) return Task.FromResult<IReadOnlyList<Azure.Messaging.ServiceBus.ServiceBusReceivedMessage>>([]);
            _delivered = true;
            OnReceive?.Invoke();
            return Task.FromResult<IReadOnlyList<Azure.Messaging.ServiceBus.ServiceBusReceivedMessage>>(Enumerable.Range(0, count)
                .Select(index => Azure.Messaging.ServiceBus.ServiceBusModelFactory.ServiceBusReceivedMessage(
                    body: BinaryData.FromString("x"), messageId: $"m-{index}", sequenceNumber: index + 1, lockTokenGuid: Guid.NewGuid()))
                .ToArray());
        }

        public override Task CompleteMessageAsync(Azure.Messaging.ServiceBus.ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
        {
            if (message.SequenceNumber - 1 == FailCompleteOf) return Task.FromException(new InvalidOperationException("lock lost"));
            Completed++;
            return Task.CompletedTask;
        }

        public override Task AbandonMessageAsync(Azure.Messaging.ServiceBus.ServiceBusReceivedMessage message,
            IDictionary<string, object>? propertiesToModify = null, CancellationToken cancellationToken = default)
        {
            Abandoned++;
            return Task.CompletedTask;
        }

        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
