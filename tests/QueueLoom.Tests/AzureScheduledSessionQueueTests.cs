using System.Reflection;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;
using ServiceBusMessageState = QueueLoom.Core.ServiceBus.ServiceBusMessageState;

namespace QueueLoom.Tests;

// Cancelling a scheduled message checked it first with a peek through a receiver without a session. Service Bus does not
// open such a receiver on a queue that requires sessions, so every scheduled message there failed to cancel, although
// cancelling by sequence number needs no session. A session queue is handled like a topic now: the browsed message is
// backed up and cancelled by its sequence number, without taking the lock of a session a consumer may hold.
public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task AzureScheduled_ACancelInASessionQueueNeedsNoNonSessionReceiver()
    {
        using var directory = new TemporaryDirectory();
        var client = new SessionQueueClient();
        await using var azure = new AzureServiceBusWorkspace(new FakeSecretVault(),
            backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(directory.Path)));
        var profile = CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        Set(azure, "_client", client);
        Set(azure, "_profile", profile);
        Set(azure, "_cachedTopology", new ServiceBusTopology(DateTimeOffset.UtcNow,
            [new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(active: 1)), RequiresSession: true)]));
        var scheduled = new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.Active, 7, "later"u8.ToArray(),
            new EditableMessageProperties(MessageId: "m-7", SessionId: "customer-1"), state: ServiceBusMessageState.Scheduled);

        var result = await azure.RemovePendingMessagesAsync([scheduled], CancellationToken.None);

        var item = Assert.Single(result.Messages);
        Assert.Equal(DeadLetterMessageDeletionOutcome.Deleted, item.Outcome);
        Assert.Equal([7L], client.Cancelled);
        Assert.Equal(0, client.NonSessionReceivers);
        var backup = Assert.Single(await new JsonDeadLetterBackupRepository(QueueLoomPaths.ForRoot(directory.Path)).ListAsync());
        Assert.Equal("m-7", backup.MessageId);
    }

    private static void Set(AzureServiceBusWorkspace azure, string field, object value) =>
        typeof(AzureServiceBusWorkspace).GetField(field, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure, value);

    private sealed class SessionQueueClient : ServiceBusClient
    {
        public List<long> Cancelled { get; } = [];
        public int NonSessionReceivers { get; private set; }

        // Service Bus refuses a receiver without a session on an entity that requires sessions.
        public override ServiceBusReceiver CreateReceiver(string queueName)
        {
            NonSessionReceivers++;
            return new RefusedReceiver();
        }

        public override ServiceBusReceiver CreateReceiver(string queueName, ServiceBusReceiverOptions options) => CreateReceiver(queueName);
        public override ServiceBusSender CreateSender(string queueOrTopicName) => new CancellingSender(this);
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class RefusedReceiver : ServiceBusReceiver
    {
        public override Task<ServiceBusReceivedMessage> PeekMessageAsync(long? fromSequenceNumber = null,
            CancellationToken cancellationToken = default) =>
            Task.FromException<ServiceBusReceivedMessage>(new InvalidOperationException(
                "It is not possible for an entity that requires sessions to create a non-sessionful message receiver."));

        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class CancellingSender(SessionQueueClient client) : ServiceBusSender
    {
        public override Task CancelScheduledMessageAsync(long sequenceNumber, CancellationToken cancellationToken = default)
        {
            client.Cancelled.Add(sequenceNumber);
            return Task.CompletedTask;
        }

        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
