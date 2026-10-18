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

// A message scheduled on a topic was cancelled by its sequence number without checking that it was still scheduled.
// Cancelling a sequence number the broker no longer holds is no error, so a message already delivered would be
// reported as deleted. It is now peeked where it is scheduled. A scheduled message stays on its topic until it is
// enqueued, and a subscription's peek does not return it (Azure's message browsing), so the topic is peeked: the fake
// subscription never returns it, as Service Bus does not.
public sealed class ScheduledCancelVerificationTests
{
    private static readonly ServiceBusEntityReference Billing = ServiceBusEntityReference.Subscription("orders", "billing");

    [Theory]
    [InlineData("scheduled", DeadLetterMessageDeletionOutcome.Deleted)]
    [InlineData("enqueued", DeadLetterMessageDeletionOutcome.NotFound)]
    [InlineData("gone", DeadLetterMessageDeletionOutcome.NotFound)]
    [InlineData("other message", DeadLetterMessageDeletionOutcome.NotFound)]
    public async Task AScheduledTopicMessageIsCheckedOnTheTopicBeforeItIsCancelled(string now, DeadLetterMessageDeletionOutcome expected)
    {
        using var directory = new TemporaryDirectory();
        var client = new Client(now);
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        await using var azure = new AzureServiceBusWorkspace(new DeepAuditCloudTests.EmptyVault(), backupStore: new DeadLetterJsonBackupStore(paths));
        var profile = ViewModelStateTests.CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        typeof(AzureServiceBusWorkspace).GetField("_client", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure, client);
        typeof(AzureServiceBusWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure, profile);
        // The browsed copy, whose body differs from the topic's fresh one: the backup must come from the fresh copy.
        var message = new BrowsedMessage(Billing, ServiceBusSubQueue.Active, 7, "browsed snapshot"u8.ToArray(),
            new EditableMessageProperties(MessageId: "m-7"), state: ServiceBusMessageState.Scheduled);

        var result = await azure.RemovePendingMessagesAsync([message]);

        Assert.Equal(expected, Assert.Single(result.Messages).Outcome);
        Assert.Equal(["orders"], client.PeekedEntities);
        var repository = new JsonDeadLetterBackupRepository(paths);
        var backups = await repository.ListAsync();
        if (expected == DeadLetterMessageDeletionOutcome.Deleted)
        {
            // Cancelled on the topic, where it is scheduled, after a backup of the fresh copy.
            Assert.Equal(("orders", 7L), Assert.Single(client.Cancelled));
            var backup = await repository.LoadAsync(Assert.Single(backups));
            Assert.Equal("fresh from the topic", System.Text.Encoding.UTF8.GetString(backup.Body.Span));
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
        public List<string> PeekedEntities { get; } = [];
        public List<(string Entity, long SequenceNumber)> Cancelled { get; } = [];
        public string State => now;

        // The single-entity receiver peeks a queue or a topic.
        public override ServiceBusReceiver CreateReceiver(string queueOrTopicName)
        {
            PeekedEntities.Add(queueOrTopicName);
            return new TopicReceiver(this);
        }
        public override ServiceBusReceiver CreateReceiver(string queueOrTopicName, ServiceBusReceiverOptions options) => CreateReceiver(queueOrTopicName);
        // A subscription never returns its topic's scheduled messages.
        public override ServiceBusReceiver CreateReceiver(string topicName, string subscriptionName) => new EmptyReceiver();
        public override ServiceBusReceiver CreateReceiver(string topicName, string subscriptionName, ServiceBusReceiverOptions options) => new EmptyReceiver();
        public override ServiceBusSender CreateSender(string queueOrTopicName) => new Sender(this, queueOrTopicName);
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class TopicReceiver(Client client) : ServiceBusReceiver
    {
        public override Task<ServiceBusReceivedMessage> PeekMessageAsync(long? fromSequenceNumber = null, CancellationToken cancellationToken = default) =>
            Task.FromResult(client.State switch
            {
                "gone" => null!,
                // Peeking from a sequence number returns the next message there is: here a later, unrelated one.
                "other message" => Message(fromSequenceNumber!.Value + 3, "m-10", AzureState.Scheduled),
                "enqueued" => Message(fromSequenceNumber!.Value, "m-7", AzureState.Active),
                _ => Message(fromSequenceNumber!.Value, "m-7", AzureState.Scheduled)
            });

        private static ServiceBusReceivedMessage Message(long number, string id, AzureState state) =>
            ServiceBusModelFactory.ServiceBusReceivedMessage(body: BinaryData.FromString("fresh from the topic"), messageId: id,
                sequenceNumber: number, serviceBusMessageState: state);

        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class EmptyReceiver : ServiceBusReceiver
    {
        public override Task<ServiceBusReceivedMessage> PeekMessageAsync(long? fromSequenceNumber = null, CancellationToken cancellationToken = default) =>
            Task.FromResult<ServiceBusReceivedMessage>(null!);
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
