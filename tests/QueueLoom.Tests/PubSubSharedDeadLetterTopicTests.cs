using System.Reflection;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Google;
using QueueLoom.Infrastructure.Messaging;

namespace QueueLoom.Tests;

// QueueLoom read a dead-letter topic through its first subscription by name, and purge or delete acknowledged there.
// With several subscriptions on that topic the others kept their copies, and when the first one belonged to another
// consumer QueueLoom took its messages. Such a topic can still be viewed, but nothing is acknowledged through it.
public sealed class PubSubSharedDeadLetterTopicTests
{
    private const BindingFlags Any = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
    private static readonly DeadLetterPolicy Policy = new() { DeadLetterTopic = "projects/p/topics/events-dlq", MaxDeliveryAttempts = 5 };

    [Fact]
    public async Task DeletingThroughADeadLetterTopicWithSeveralSubscriptionsIsRefused()
    {
        var subscriber = new AckRecordingSubscriber();
        var channel = OpenDeadLetterChannel(subscriber, "alerts-consumer", "dlq-reader");
        var leased = await channel.ReceiveAsync(10, CancellationToken.None);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => channel.SettleAsync(leased, CancellationToken.None));

        Assert.Empty(subscriber.AcknowledgedIds);
        Assert.Contains("alerts-consumer, dlq-reader", error.Message, StringComparison.Ordinal);
        Assert.Equal(["a1"], subscriber.ReleasedIds);
    }

    [Fact]
    public async Task ASingleReaderIsStillUsedToDelete()
    {
        var subscriber = new AckRecordingSubscriber();
        var channel = OpenDeadLetterChannel(subscriber, "dlq-reader");
        var leased = await channel.ReceiveAsync(10, CancellationToken.None);

        var unsettled = await channel.SettleAsync(leased, CancellationToken.None);

        Assert.Empty(unsettled);
        Assert.Equal(["a1"], subscriber.AcknowledgedIds);
    }

    [Fact]
    public void TheSourceSaysItsDeadLetterTopicIsShared()
    {
        var index = Build("alerts-consumer", "dlq-reader");

        var orders = index.Topology.Topics.Single(topic => topic.Name == "events").Subscriptions.Single();

        Assert.Contains("read-only", orders.Note, StringComparison.Ordinal);
    }

    private static GooglePubSubTopology Build(params string[] readers) => GooglePubSubTopology.Build("p", ["events", "events-dlq"],
        [
            new Subscription { Name = "projects/p/subscriptions/orders", Topic = "projects/p/topics/events", DeadLetterPolicy = Policy },
            .. readers.Select(reader => new Subscription { Name = $"projects/p/subscriptions/{reader}", Topic = "projects/p/topics/events-dlq" })
        ], DateTimeOffset.UtcNow);

    private static ILeasedMessageChannel OpenDeadLetterChannel(AckRecordingSubscriber subscriber, params string[] readers)
    {
        var index = Build(readers);
        var workspace = new GooglePubSubWorkspace(new DeepAuditCloudTests.EmptyVault());
        typeof(GooglePubSubWorkspace).GetField("_subscriber", Any)!.SetValue(workspace, subscriber);
        typeof(GooglePubSubWorkspace).GetField("_subscriptions", Any)!.SetValue(workspace, index.Subscriptions);
        typeof(GooglePubSubWorkspace).GetField("_deadLetterReaders", Any)!.SetValue(workspace, index.DeadLetterReaders);
        typeof(GooglePubSubWorkspace).GetField("_sharedDeadLetterTopics", Any)!.SetValue(workspace, index.SharedDeadLetterTopics);
        var open = typeof(GooglePubSubWorkspace).GetMethod("OpenChannel", Any)!;
        return (ILeasedMessageChannel)open.Invoke(workspace,
            [index.Topology, ServiceBusEntityReference.Subscription("events", "orders"), ServiceBusSubQueue.DeadLetter])!;
    }

    private sealed class AckRecordingSubscriber : SubscriberServiceApiClient
    {
        public List<string> AcknowledgedIds { get; } = [];
        public List<string> ReleasedIds { get; } = [];

        public override Task<PullResponse> PullAsync(PullRequest request, CallSettings? callSettings = null) =>
            Task.FromResult(new PullResponse
            {
                ReceivedMessages =
                {
                    new ReceivedMessage
                    {
                        AckId = "a1",
                        Message = new PubsubMessage { MessageId = "m1", Attributes = { ["CloudPubSubDeadLetterSourceSubscription"] = "orders" } }
                    }
                }
            });

        public override Task ModifyAckDeadlineAsync(ModifyAckDeadlineRequest request, CallSettings? callSettings = null)
        {
            if (request.AckDeadlineSeconds == 0) ReleasedIds.AddRange(request.AckIds);
            return Task.CompletedTask;
        }

        public override Task AcknowledgeAsync(SubscriptionName subscription, IEnumerable<string> ackIds, CallSettings? callSettings = null)
        {
            AcknowledgedIds.AddRange(ackIds);
            return Task.CompletedTask;
        }
    }
}
