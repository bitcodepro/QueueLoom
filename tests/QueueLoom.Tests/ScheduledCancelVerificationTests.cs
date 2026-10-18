using System.Reflection;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;
using AzureState = Azure.Messaging.ServiceBus.ServiceBusMessageState;
using ServiceBusMessageState = QueueLoom.Core.ServiceBus.ServiceBusMessageState;

namespace QueueLoom.Tests;

// A message scheduled on a topic, read through one of its subscriptions, was cancelled by its sequence number without
// checking that it was still scheduled. Cancelling a sequence number the broker no longer holds is no error, so a
// message already delivered to consumers was reported as deleted. It is now checked first, as on a queue.
public sealed class ScheduledCancelVerificationTests
{
    private static readonly ServiceBusEntityReference Billing = ServiceBusEntityReference.Subscription("orders", "billing");

    [Theory]
    [InlineData("scheduled", DeadLetterMessageDeletionOutcome.Deleted)]
    [InlineData("delivered", DeadLetterMessageDeletionOutcome.NotFound)]
    [InlineData("gone", DeadLetterMessageDeletionOutcome.NotFound)]
    [InlineData("other message", DeadLetterMessageDeletionOutcome.NotFound)]
    public async Task AScheduledTopicMessageIsCheckedThroughItsSubscriptionBeforeItIsCancelled(string now, DeadLetterMessageDeletionOutcome expected)
    {
        using var directory = new TemporaryDirectory();
        var client = new Client(now);
        await using var azure = new AzureServiceBusWorkspace(new DeepAuditCloudTests.EmptyVault(), backupStore:
            new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(directory.Path)));
        var profile = ViewModelStateTests.CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        typeof(AzureServiceBusWorkspace).GetField("_client", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure, client);
        typeof(AzureServiceBusWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure, profile);
        var message = new BrowsedMessage(Billing, ServiceBusSubQueue.Active, 7, "scheduled"u8.ToArray(),
            new EditableMessageProperties(MessageId: "m-7"), state: ServiceBusMessageState.Scheduled);

        var result = await azure.RemovePendingMessagesAsync([message]);

        Assert.Equal(expected, Assert.Single(result.Messages).Outcome);
        Assert.Equal(("orders", "billing"), client.PeekedThrough);
        var backups = await new JsonDeadLetterBackupRepository(QueueLoomPaths.ForRoot(directory.Path)).ListAsync();
        if (expected == DeadLetterMessageDeletionOutcome.Deleted)
        {
            // Cancelled on the topic, where it is scheduled, after its backup.
            Assert.Equal(("orders", 7L), Assert.Single(client.Cancelled));
            Assert.Equal("m-7", Assert.Single(backups).MessageId);
        }
        else
        {
            Assert.Empty(client.Cancelled);
            Assert.Empty(backups);
            Assert.Contains("no longer scheduled", Assert.Single(result.Messages).Detail, StringComparison.Ordinal);
        }
    }

    private sealed class Client(string now) : ServiceBusClient
    {
        public (string Topic, string Subscription)? PeekedThrough { get; set; }
        public List<(string Entity, long SequenceNumber)> Cancelled { get; } = [];
        public string State => now;
        public override ServiceBusReceiver CreateReceiver(string topicName, string subscriptionName)
        {
            PeekedThrough = (topicName, subscriptionName);
            return new Receiver(this);
        }
        public override ServiceBusReceiver CreateReceiver(string topicName, string subscriptionName, ServiceBusReceiverOptions options) =>
            CreateReceiver(topicName, subscriptionName);
        public override ServiceBusReceiver CreateReceiver(string queueName) =>
            throw new InvalidOperationException("A subscription's message is read through the subscription.");
        public override ServiceBusSender CreateSender(string queueOrTopicName) => new Sender(this, queueOrTopicName);
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class Receiver(Client client) : ServiceBusReceiver
    {
        public override Task<ServiceBusReceivedMessage> PeekMessageAsync(long? fromSequenceNumber = null, CancellationToken cancellationToken = default) =>
            Task.FromResult(client.State switch
            {
                "gone" => null!,
                // Peeking from a sequence number returns the next message there is: here a later, unrelated one.
                "other message" => Message(fromSequenceNumber!.Value + 3, "m-10", AzureState.Active),
                "delivered" => Message(fromSequenceNumber!.Value, "m-7", AzureState.Active),
                _ => Message(fromSequenceNumber!.Value, "m-7", AzureState.Scheduled)
            });

        private static ServiceBusReceivedMessage Message(long number, string id, AzureState state) =>
            ServiceBusModelFactory.ServiceBusReceivedMessage(body: BinaryData.FromString("scheduled"), messageId: id,
                sequenceNumber: number, serviceBusMessageState: state);

        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class Sender(Client client, string entity) : ServiceBusSender
    {
        public override Task CancelScheduledMessageAsync(long sequenceNumber, CancellationToken cancellationToken = default)
        {
            client.Cancelled.Add((entity, sequenceNumber));
            return Task.CompletedTask;
        }
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
