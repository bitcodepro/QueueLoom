using System.Reflection;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Google;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// QueueLoom read a dead-letter topic through its first subscription by name, and purge, delete and move acknowledged
// there. With several subscriptions on that topic the others kept their copies, and when the first one belonged to
// another consumer QueueLoom took its messages. Right before any deletion or move it now lists every subscription
// attached to the topic, in any project and over every page; more than one, or a listing it could not complete,
// refuses the whole operation before anything is backed up, published or acknowledged. Viewing still works.
public sealed class PubSubSharedDeadLetterTopicTests : IDisposable
{
    private const BindingFlags Any = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
    private const string DeadLetterTopic = "projects/p/topics/events-dlq";
    private static readonly ServiceBusEntityReference Orders = ServiceBusEntityReference.Subscription("events", "orders");
    private static readonly ServiceBusEntityReference Billing = ServiceBusEntityReference.Subscription("events", "billing");
    private readonly TemporaryDirectory _directory = new();

    public void Dispose() => _directory.Dispose();

    // Another project's subscription reads the topic too; this project's listing (the cached topology) shows only one.
    [Fact]
    public async Task APurgeIsRefusedWhenAnotherProjectAlsoReadsTheTopic()
    {
        var (workspace, publisher, subscriber) = Connect([["projects/p/subscriptions/dlq-reader", "projects/q/subscriptions/audit"]]);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest([Orders], [ServiceBusSubQueue.DeadLetter])));

        Assert.Contains("projects/q/subscriptions/audit", error.Message, StringComparison.Ordinal);
        Assert.Equal(0, subscriber.Pulls);
        Assert.Empty(subscriber.AcknowledgedIds);
        Assert.Equal([DeadLetterTopic], publisher.ListedTopics);
        Assert.False(Directory.Exists(Paths.BackupsDirectory), "A backup was started for a refused purge.");
    }

    [Fact]
    public async Task ReadersOnALaterPageCount()
    {
        var (workspace, _, subscriber) = Connect([["projects/p/subscriptions/dlq-reader"], ["projects/q/subscriptions/audit"]]);

        await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.DeleteDeadLetterMessagesAsync(
            new DeleteDeadLetterMessagesRequest([new DeadLetterMessageKey(Orders, ServiceBusSubQueue.DeadLetter, 1, "m1")])));

        Assert.Empty(subscriber.AcknowledgedIds);
    }

    // A listing that could not be completed never authorizes a deletion.
    [Fact]
    public async Task AListingThatFailsRefusesTheDeletion()
    {
        var (workspace, _, subscriber) = Connect([["projects/p/subscriptions/dlq-reader"]], listFailure: StatusCode.PermissionDenied);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest([Orders], [ServiceBusSubQueue.DeadLetter])));

        Assert.Contains("could not list", error.Message, StringComparison.Ordinal);
        Assert.Empty(subscriber.AcknowledgedIds);
    }

    // One attached reader (a detached one is not listed by the topic and holds nothing): the purge proceeds.
    [Fact]
    public async Task ASingleAttachedReaderStillPurges()
    {
        var (workspace, _, subscriber) = Connect([["projects/p/subscriptions/dlq-reader"]]);

        var result = await workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest([Orders], [ServiceBusSubQueue.DeadLetter]));

        Assert.Equal(1, result.Sources.Single().DeletedCount);
        Assert.Equal(["a1"], subscriber.AcknowledgedIds);
    }

    // A move of messages from a permitted source and a shared one publishes nothing at all.
    [Fact]
    public async Task AMoveIsRefusedBeforeItsFirstPublish()
    {
        var (workspace, publisher, subscriber) = Connect([["projects/p/subscriptions/dlq-reader", "projects/q/subscriptions/audit"]]);
        var items = new[] { Item(Billing, 1), Item(Orders, 2) };

        await Assert.ThrowsAsync<InvalidOperationException>(() => DeadLetterResender.ResendAsync(workspace, items, ResendMode.Move));

        Assert.Equal(0, publisher.Published);
        Assert.Empty(subscriber.AcknowledgedIds);
    }

    // The saved (durable) move checks before its first send too: nothing is published and every item stays Pending, not
    // "DeleteUncertain".
    [Fact]
    public async Task ASavedMoveIsRefusedBeforeItsFirstPublish()
    {
        var (workspace, publisher, subscriber) = Connect([["projects/p/subscriptions/dlq-reader", "projects/q/subscriptions/audit"]]);
        var store = new BatchReplayStore(Path.Combine(_directory.Path, "replays"));
        var plan = await store.CreateResendAsync(workspace.ConnectedProfileId!.Value, [Item(Billing, 1).WithNewMessageId(), Item(Orders, 2).WithNewMessageId()],
            ResendMode.Move, 50, workspace.ConnectedNamespace, workspace.ConnectedConfigurationIdentity ?? string.Empty, "resend", default);

        await Assert.ThrowsAsync<InvalidOperationException>(() => store.RunItemsAsync(plan, [0, 1], false, workspace, () => true, null, default));

        Assert.Equal(0, publisher.Published);
        Assert.Empty(subscriber.AcknowledgedIds);
        Assert.All(store.ReadHistory(plan).Items, item => Assert.Equal("Pending", item.State));
    }

    [Fact]
    public async Task ACopyOfASharedTopicsMessagesIsStillPublished()
    {
        var (workspace, publisher, _) = Connect([["projects/p/subscriptions/dlq-reader", "projects/q/subscriptions/audit"]]);

        var result = await DeadLetterResender.ResendAsync(workspace, [Item(Orders, 1)], ResendMode.Copy);

        Assert.Equal(ResendOutcome.Sent, result.Items.Single().Outcome);
        Assert.Equal(1, publisher.Published);
    }

    // A channel opened while one reader was known loses its permission as soon as a check finds a second reader.
    [Fact]
    public async Task AnOpenChannelFollowsTheLatestCheck()
    {
        var (workspace, publisher, subscriber) = Connect([["projects/p/subscriptions/dlq-reader"]]);
        var channel = OpenChannel(workspace, Orders);
        var leased = await channel.ReceiveAsync(10, CancellationToken.None);
        publisher.Pages = [["projects/p/subscriptions/dlq-reader", "projects/q/subscriptions/audit"]];

        await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.EnsureDeletableAsync([(Orders, ServiceBusSubQueue.DeadLetter)]));
        await Assert.ThrowsAsync<InvalidOperationException>(() => channel.SettleAsync(leased, CancellationToken.None));

        Assert.Empty(subscriber.AcknowledgedIds);
        Assert.Equal(["a1"], subscriber.ReleasedIds);
    }

    // Viewing a shared topic's messages is unaffected, and the message is attributed to its source.
    [Fact]
    public async Task ASharedTopicCanStillBeViewed()
    {
        var (workspace, _, _) = Connect([["projects/p/subscriptions/dlq-reader", "projects/q/subscriptions/audit"]]);

        var messages = await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(Orders, ServiceBusSubQueue.DeadLetter, 10));

        Assert.Equal("m1", Assert.Single(messages).Properties.MessageId);
    }

    [Fact]
    public void TheSourceSaysItsDeadLetterTopicIsShared()
    {
        var index = Build("projects/p/subscriptions/dlq-reader", "projects/p/subscriptions/audit");

        var orders = index.Topology.Topics.Single(topic => topic.Name == "events").Subscriptions.Single(subscription => subscription.Name == "orders");

        Assert.Contains("read-only", orders.Note, StringComparison.Ordinal);
    }

    private QueueLoomPaths Paths => QueueLoomPaths.ForRoot(_directory.Path);

    private static ResendItem Item(ServiceBusEntityReference source, long sequence)
    {
        var original = new BrowsedMessage(source, ServiceBusSubQueue.DeadLetter, sequence, "x"u8.ToArray(), new EditableMessageProperties(MessageId: $"m{sequence}"));
        return new ResendItem(original, ServiceBusEntityReference.Topic("events"), original.CreateDraft());
    }

    private const string BillingTopic = "projects/p/topics/billing-dlq";

    private static GooglePubSubTopology Build(params string[] readers) => GooglePubSubTopology.Build("p", ["events", "events-dlq", "billing-dlq"],
        [
            new Subscription { Name = "projects/p/subscriptions/orders", Topic = "projects/p/topics/events", DeadLetterPolicy = new DeadLetterPolicy { DeadLetterTopic = DeadLetterTopic, MaxDeliveryAttempts = 5 } },
            new Subscription { Name = "projects/p/subscriptions/billing", Topic = "projects/p/topics/events", DeadLetterPolicy = new DeadLetterPolicy { DeadLetterTopic = BillingTopic, MaxDeliveryAttempts = 5 } },
            new Subscription { Name = "projects/p/subscriptions/billing-dlq-reader", Topic = BillingTopic },
            .. readers.Where(reader => reader.StartsWith("projects/p/", StringComparison.Ordinal))
                .Select(reader => new Subscription { Name = reader, Topic = DeadLetterTopic })
        ], DateTimeOffset.UtcNow);

    private (GooglePubSubWorkspace Workspace, FakePublisher Publisher, FakeSubscriber Subscriber) Connect(string[][] pages, StatusCode? listFailure = null)
    {
        // The cached topology knows this project's readers only.
        var index = Build(pages.SelectMany(page => page).ToArray());
        var publisher = new FakePublisher { Pages = pages, ListFailure = listFailure };
        var subscriber = new FakeSubscriber();
        var workspace = new GooglePubSubWorkspace(new DeepAuditCloudTests.EmptyVault(), backupStore: new DeadLetterJsonBackupStore(Paths));
        var profile = ViewModelStateTests.CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite) with { Provider = MessagingProvider.GooglePubSub };
        Set(typeof(GooglePubSubWorkspace), workspace, "_publisher", publisher);
        Set(typeof(GooglePubSubWorkspace), workspace, "_subscriber", subscriber);
        Set(typeof(GooglePubSubWorkspace), workspace, "_projectId", "p");
        Set(typeof(GooglePubSubWorkspace), workspace, "_subscriptions", index.Subscriptions);
        Set(typeof(GooglePubSubWorkspace), workspace, "_deadLetterReaders", index.DeadLetterReaders);
        Set(typeof(LeasedMessagingWorkspace), workspace, "_profile", profile);
        Set(typeof(LeasedMessagingWorkspace), workspace, "_connectionState", WorkspaceConnectionState.Connected);
        Set(typeof(LeasedMessagingWorkspace), workspace, "_cachedTopology", index.Topology);
        return (workspace, publisher, subscriber);
    }

    private static ILeasedMessageChannel OpenChannel(GooglePubSubWorkspace workspace, ServiceBusEntityReference source) =>
        (ILeasedMessageChannel)typeof(GooglePubSubWorkspace).GetMethod("OpenChannel", Any)!.Invoke(workspace,
            [typeof(LeasedMessagingWorkspace).GetField("_cachedTopology", Any)!.GetValue(workspace), source, ServiceBusSubQueue.DeadLetter])!;

    private static void Set(Type type, object target, string name, object value) => type.GetField(name, Any)!.SetValue(target, value);

    private sealed class FakePublisher : PublisherServiceApiClient
    {
        public string[][] Pages { get; set; } = [];
        public StatusCode? ListFailure { get; init; }
        public List<string> ListedTopics { get; } = [];
        public int Published { get; private set; }

        public override PagedAsyncEnumerable<ListTopicSubscriptionsResponse, string> ListTopicSubscriptionsAsync(
            ListTopicSubscriptionsRequest request, CallSettings? callSettings = null)
        {
            ListedTopics.Add(request.Topic);
            if (ListFailure is { } failure) throw new RpcException(new Status(failure, "listing refused"));
            var pages = request.Topic == DeadLetterTopic ? Pages : [["projects/p/subscriptions/billing-dlq-reader"]];
            return new FakePaged(pages.Select(names => new ListTopicSubscriptionsResponse { Subscriptions = { names } }).ToArray());
        }

        public override Task<PublishResponse> PublishAsync(PublishRequest request, CallSettings? callSettings = null)
        {
            Published += request.Messages.Count;
            return Task.FromResult(new PublishResponse { MessageIds = { request.Messages.Select(_ => "published") } });
        }
    }

    private sealed class FakePaged(ListTopicSubscriptionsResponse[] pages) : PagedAsyncEnumerable<ListTopicSubscriptionsResponse, string>
    {
        public override IAsyncEnumerable<ListTopicSubscriptionsResponse> AsRawResponses() => Raw();

        public override Task<Page<string>> ReadPageAsync(int pageSize, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public override async IAsyncEnumerator<string> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            await foreach (var page in Raw().WithCancellation(cancellationToken))
                foreach (var name in page.Subscriptions) yield return name;
        }

        private async IAsyncEnumerable<ListTopicSubscriptionsResponse> Raw()
        {
            foreach (var page in pages) { await Task.Yield(); yield return page; }
        }
    }

    private sealed class FakeSubscriber : SubscriberServiceApiClient
    {
        private bool _delivered;
        public int Pulls { get; private set; }
        public List<string> AcknowledgedIds { get; } = [];
        public List<string> ReleasedIds { get; } = [];

        // One dead letter of "orders", attributed with both its subscription and project, then nothing.
        public override Task<PullResponse> PullAsync(PullRequest request, CallSettings? callSettings = null)
        {
            Pulls++;
            var response = new PullResponse();
            if (!_delivered)
            {
                _delivered = true;
                response.ReceivedMessages.Add(new ReceivedMessage
                {
                    AckId = "a1",
                    Message = new PubsubMessage
                    {
                        MessageId = "m1",
                        Data = Google.Protobuf.ByteString.CopyFromUtf8("x"),
                        Attributes =
                        {
                            [GooglePubSubWorkspace.DeadLetterSourceSubscription] = "orders",
                            [GooglePubSubWorkspace.DeadLetterSourceSubscriptionProject] = "p"
                        }
                    }
                });
            }
            return Task.FromResult(response);
        }

        public override Task ModifyAckDeadlineAsync(ModifyAckDeadlineRequest request, CallSettings? callSettings = null)
        {
            if (request.AckDeadlineSeconds == 0)
            {
                ReleasedIds.AddRange(request.AckIds);
                _delivered = false; // released: it can be pulled again
            }
            return Task.CompletedTask;
        }

        public override Task AcknowledgeAsync(SubscriptionName subscription, IEnumerable<string> ackIds, CallSettings? callSettings = null)
        {
            AcknowledgedIds.AddRange(ackIds);
            return Task.CompletedTask;
        }
    }
}
