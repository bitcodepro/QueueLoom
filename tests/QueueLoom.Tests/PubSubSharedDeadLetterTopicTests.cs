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

    // One attached reader plus a detached one (the topic does not list it, and it holds nothing): the purge proceeds.
    [Fact]
    public async Task ASingleAttachedReaderStillPurges()
    {
        var (workspace, _, subscriber) = Connect([["projects/p/subscriptions/dlq-reader"]], detached: ["projects/p/subscriptions/aaa-old-reader"]);

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

    // A detached subscription is neither the reader nor a second reader, even when it sorts first.
    [Fact]
    public void ADetachedSubscriptionIsNotAReader()
    {
        var index = Build(["projects/p/subscriptions/dlq-reader"], detached: ["projects/p/subscriptions/aaa-old-reader"]);

        Assert.Empty(index.SharedDeadLetterTopics);
        Assert.Equal("dlq-reader", index.DeadLetterReaders[DeadLetterTopic].SubscriptionName.SubscriptionId);
        var orders = index.Topology.Topics.Single(topic => topic.Name == "events").Subscriptions.Single(subscription => subscription.Name == "orders");
        Assert.DoesNotContain("read-only", orders.Note, StringComparison.Ordinal);
    }

    // The application moves through MultiProviderWorkspace: the check must reach the provider through it, for ordinary
    // and saved moves alike.
    [Fact]
    public async Task TheApplicationsWorkspaceForwardsTheCheck()
    {
        var (provider, publisher, subscriber) = Connect([["projects/p/subscriptions/dlq-reader", "projects/q/subscriptions/audit"]]);
        await using var router = new MultiProviderWorkspace(_ => provider);
        typeof(MultiProviderWorkspace).GetField("_current", Any)!.SetValue(router, provider);
        var store = new BatchReplayStore(Path.Combine(_directory.Path, "replays"));
        var plan = await store.CreateResendAsync(router.ConnectedProfileId!.Value, [Item(Orders, 2).WithNewMessageId()],
            ResendMode.Move, 50, router.ConnectedNamespace, router.ConnectedConfigurationIdentity ?? string.Empty, "resend", default);

        await Assert.ThrowsAsync<InvalidOperationException>(() => DeadLetterResender.ResendAsync(router, [Item(Orders, 1)], ResendMode.Move));
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.RunItemsAsync(plan, [0], false, router, () => true, null, default));

        Assert.Equal(0, publisher.Published);
        Assert.Empty(subscriber.AcknowledgedIds);
        Assert.Equal("Pending", Assert.Single(store.ReadHistory(plan).Items).State);
    }

    // Right after connecting nothing is listed yet: the check reads the subscription from the service and refuses
    // rather than passing for want of a cached entry.
    [Fact]
    public async Task ACheckBeforeAnyListingRefuses()
    {
        var (workspace, publisher, _) = Connect([["projects/p/subscriptions/dlq-reader"]]);
        Set(typeof(GooglePubSubWorkspace), workspace, "_subscriptions", new Dictionary<(string, string), Subscription>());

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.EnsureDeletableAsync([(Orders, ServiceBusSubQueue.DeadLetter)]));

        Assert.Contains("Refresh", error.Message, StringComparison.Ordinal);
        Assert.Equal(0, publisher.Published);
    }

    // The cached topology says orders dead-letters to A (one reader); the service now says B. The move is refused
    // before publishing, instead of checking A and failing at deletion on B.
    [Fact]
    public async Task AChangedDeadLetterPolicyIsRefusedBeforePublishing()
    {
        var (workspace, publisher, subscriber) = Connect([["projects/p/subscriptions/dlq-reader"]]);
        subscriber.Policies["orders"] = "projects/p/topics/shared-dlq";

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => DeadLetterResender.ResendAsync(workspace, [Item(Orders, 1)], ResendMode.Move));

        Assert.Contains("shared-dlq", error.Message, StringComparison.Ordinal);
        Assert.Equal(0, publisher.Published);
        Assert.DoesNotContain("projects/p/topics/shared-dlq", publisher.ListedTopics);
    }

    // Check A reads one reader and is held; check B, started later, finds two and refuses; A then finishes. A's older
    // observation must not clear B's refusal, so an open channel still may not acknowledge.
    [Fact]
    public async Task AnOlderCheckDoesNotClearANewerRefusal()
    {
        var (workspace, publisher, subscriber) = Connect([["projects/p/subscriptions/dlq-reader"]]);
        var channel = OpenChannel(workspace, Orders);
        var leased = await channel.ReceiveAsync(10, CancellationToken.None);
        var gate = new Gate();
        publisher.BeforeLastPage = () => { publisher.BeforeLastPage = null; return gate.Hold(); };

        var checkA = workspace.EnsureDeletableAsync([(Orders, ServiceBusSubQueue.DeadLetter)]);
        await gate.Run(checkA, async () =>
        {
            publisher.Pages = [["projects/p/subscriptions/dlq-reader", "projects/q/subscriptions/audit"]];
            await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.EnsureDeletableAsync([(Orders, ServiceBusSubQueue.DeadLetter)]));
        });
        await Assert.ThrowsAsync<InvalidOperationException>(() => checkA);

        await Assert.ThrowsAsync<InvalidOperationException>(() => channel.SettleAsync(leased, CancellationToken.None));
        Assert.Empty(subscriber.AcknowledgedIds);
    }

    // Check A is held reading the subscription; check B completes with one reader (permission); then a second reader
    // appears (or the listing starts failing), and A resumes. A's own refusal stands although B's permission is newer.
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AChecksOwnRefusalStandsAfterANewerPermission(bool listingFails)
    {
        var (workspace, publisher, subscriber) = Connect([["projects/p/subscriptions/dlq-reader"]]);
        var gate = new Gate();
        subscriber.BeforeGet = _ => { subscriber.BeforeGet = null; return gate.Hold(); };

        var checkA = DeadLetterResender.ResendAsync(workspace, [Item(Orders, 1)], ResendMode.Move);
        await gate.Run(checkA, async () =>
        {
            await workspace.EnsureDeletableAsync([(Orders, ServiceBusSubQueue.DeadLetter)]); // B: one reader, allowed
            if (listingFails) publisher.ListFailure = StatusCode.Unavailable;
            else publisher.Pages = [["projects/p/subscriptions/dlq-reader", "projects/q/subscriptions/audit"]];
        });

        await Assert.ThrowsAsync<InvalidOperationException>(() => checkA);
        Assert.Equal(0, publisher.Published);
        Assert.Empty(subscriber.AcknowledgedIds);
    }

    // Check A is held reading the subscription. Move B then reads one reader and is held before its listing completes.
    // A second reader appears (or the listing starts failing), A resumes and records a refusal, then B resumes with its
    // one-reader result. B began before A's refusal was recorded, so it cannot clear it: B is refused, publishes
    // nothing, and a channel already open still may not acknowledge. Ordinary and saved moves alike.
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task AnInFlightPassCannotEraseARefusalRecordedMeanwhile(bool listingFails, bool durable)
    {
        var (workspace, publisher, subscriber) = Connect([["projects/p/subscriptions/dlq-reader"]]);
        var channel = OpenChannel(workspace, Orders);
        var leased = await channel.ReceiveAsync(10, CancellationToken.None);
        var store = new BatchReplayStore(Path.Combine(_directory.Path, "replays"));
        var plan = await store.CreateResendAsync(workspace.ConnectedProfileId!.Value, [Item(Orders, 2).WithNewMessageId()],
            ResendMode.Move, 50, workspace.ConnectedNamespace, workspace.ConnectedConfigurationIdentity ?? string.Empty, "resend", default);
        var gateA = new Gate();
        var gateB = new Gate();
        subscriber.BeforeGet = _ => { subscriber.BeforeGet = null; return gateA.Hold(); };

        var checkA = workspace.EnsureDeletableAsync([(Orders, ServiceBusSubQueue.DeadLetter)]);
        Task? moveB = null;
        await gateA.Run(checkA, async () =>
        {
            publisher.BeforeLastPage = () => { publisher.BeforeLastPage = null; return gateB.Hold(); };
            moveB = durable
                ? store.RunItemsAsync(plan, [0], false, workspace, () => true, null, default)
                : DeadLetterResender.ResendAsync(workspace, [Item(Orders, 1)], ResendMode.Move);
            await gateB.Run(moveB, async () =>
            {
                if (listingFails) publisher.ListFailure = StatusCode.Unavailable;
                else publisher.Pages = [["projects/p/subscriptions/dlq-reader", "projects/q/subscriptions/audit"]];
                gateA.Release();
                await Assert.ThrowsAsync<InvalidOperationException>(() => checkA.WaitAsync(TimeSpan.FromSeconds(10)));
            });
        });

        await Assert.ThrowsAsync<InvalidOperationException>(() => moveB!);
        Assert.Equal(0, publisher.Published);
        await Assert.ThrowsAsync<InvalidOperationException>(() => channel.SettleAsync(leased, CancellationToken.None));
        Assert.Empty(subscriber.AcknowledgedIds);
        if (durable) Assert.Equal("Pending", Assert.Single(store.ReadHistory(plan).Items).State);
    }

    // A refusal recorded earlier (the topic was shared) is cleared by a check that begins after it and finds the one
    // reader again: the move then goes ahead.
    [Fact]
    public async Task AFreshCheckClearsAnObsoleteRefusal()
    {
        var (workspace, publisher, subscriber) = Connect([["projects/p/subscriptions/dlq-reader", "projects/q/subscriptions/audit"]]);
        await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.EnsureDeletableAsync([(Orders, ServiceBusSubQueue.DeadLetter)]));
        publisher.Pages = [["projects/p/subscriptions/dlq-reader"]];
        subscriber.All = Subscriptions(["projects/p/subscriptions/dlq-reader"], null);

        var result = await DeadLetterResender.ResendAsync(workspace, [Item(Orders, 1)], ResendMode.Move);

        Assert.Equal(ResendOutcome.Moved, result.Items.Single().Outcome);
        Assert.Equal(["a1"], subscriber.AcknowledgedIds);
    }

    // A two-source move passes orders, then is held on billing; meanwhile another check records that orders' topic is
    // shared. Before the move is admitted every source's latest state is consulted again, so it is refused.
    [Fact]
    public async Task AMultiSourceCheckRevisitsEverySourceBeforeAdmittingTheMove()
    {
        var (workspace, publisher, subscriber) = Connect([["projects/p/subscriptions/dlq-reader"]]);
        var gate = new Gate();
        subscriber.BeforeGet = id => id == "billing" ? Take(gate) : Task.CompletedTask;
        Task Take(Gate held) { subscriber.BeforeGet = null; return held.Hold(); }

        var move = DeadLetterResender.ResendAsync(workspace, [Item(Orders, 1), Item(Billing, 2)], ResendMode.Move);
        await gate.Run(move, async () =>
        {
            publisher.Pages = [["projects/p/subscriptions/dlq-reader", "projects/q/subscriptions/audit"]];
            await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.EnsureDeletableAsync([(Orders, ServiceBusSubQueue.DeadLetter)]));
        });

        await Assert.ThrowsAsync<InvalidOperationException>(() => move);
        Assert.Equal(0, publisher.Published);
        Assert.Empty(subscriber.AcknowledgedIds);
    }

    // A purge opens its channel on topic A, then is held reading the subscription while the policy moves to B and the
    // cache is refreshed to B. The check is bound to the channel's A, so it refuses before anything is received.
    [Fact]
    public async Task APurgeIsCheckedAgainstItsOwnChannelNotTheRefreshedCache()
    {
        var (workspace, publisher, subscriber) = Connect([["projects/p/subscriptions/dlq-reader"]]);
        var gate = new Gate();
        subscriber.BeforeGet = _ => { subscriber.BeforeGet = null; return gate.Hold(); };

        var purge = workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest([Orders], [ServiceBusSubQueue.DeadLetter]));
        await gate.Run(purge, () =>
        {
            const string other = "projects/p/topics/other-dlq";
            subscriber.Policies["orders"] = other;
            var refreshed = GooglePubSubTopology.Build("p", ["events", "other-dlq"],
            [
                new Subscription { Name = "projects/p/subscriptions/orders", Topic = "projects/p/topics/events", DeadLetterPolicy = new DeadLetterPolicy { DeadLetterTopic = other, MaxDeliveryAttempts = 5 } },
                new Subscription { Name = "projects/p/subscriptions/other-reader", Topic = other }
            ], DateTimeOffset.UtcNow);
            Set(typeof(GooglePubSubWorkspace), workspace, "_subscriptions", refreshed.Subscriptions);
            Set(typeof(GooglePubSubWorkspace), workspace, "_deadLetterReaders", refreshed.DeadLetterReaders);
            publisher.OtherTopicReaders["projects/p/topics/other-dlq"] = ["projects/p/subscriptions/other-reader"];
            return Task.CompletedTask;
        });

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => purge);
        Assert.Contains("other-dlq", error.Message, StringComparison.Ordinal);
        Assert.Equal(0, subscriber.Pulls);
        Assert.Empty(subscriber.AcknowledgedIds);
    }

    // The reader QueueLoom reads through is gone (no reader now) or replaced by another project's subscription: a move
    // is refused before publishing, through the application's workspace, ordinary and saved alike.
    [Theory]
    [InlineData("")]
    [InlineData("projects/q/subscriptions/someone-else")]
    public async Task AMoveNeedsTheVeryReaderItWillDeleteThrough(string readerNow)
    {
        var (provider, publisher, subscriber) = Connect([["projects/p/subscriptions/dlq-reader"]]);
        publisher.Pages = [readerNow.Length == 0 ? [] : [readerNow]];
        await using var router = new MultiProviderWorkspace(_ => provider);
        typeof(MultiProviderWorkspace).GetField("_current", Any)!.SetValue(router, provider);
        var store = new BatchReplayStore(Path.Combine(_directory.Path, "replays"));
        var plan = await store.CreateResendAsync(router.ConnectedProfileId!.Value, [Item(Orders, 2).WithNewMessageId()],
            ResendMode.Move, 50, router.ConnectedNamespace, router.ConnectedConfigurationIdentity ?? string.Empty, "resend", default);

        await Assert.ThrowsAsync<InvalidOperationException>(() => DeadLetterResender.ResendAsync(router, [Item(Orders, 1)], ResendMode.Move));
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.RunItemsAsync(plan, [0], false, router, () => true, null, default));

        Assert.Equal(0, publisher.Published);
        Assert.Empty(subscriber.AcknowledgedIds);
        Assert.Equal("Pending", Assert.Single(store.ReadHistory(plan).Items).State);
    }

    // With its one reader in place, a move through the application's workspace publishes and deletes.
    [Fact]
    public async Task AMoveWithItsOneReaderSucceeds()
    {
        var (provider, publisher, subscriber) = Connect([["projects/p/subscriptions/dlq-reader"]]);
        await using var router = new MultiProviderWorkspace(_ => provider);
        typeof(MultiProviderWorkspace).GetField("_current", Any)!.SetValue(router, provider);

        var result = await DeadLetterResender.ResendAsync(router, [Item(Orders, 1)], ResendMode.Move);

        Assert.Equal(1, publisher.Published);
        Assert.Equal(["a1"], subscriber.AcknowledgedIds);
        Assert.Equal(ResendOutcome.Moved, result.Items.Single().Outcome);
    }

    /// <summary>
    /// Holds one operation at a known point. Every wait is bounded; the held operation is always released and observed,
    /// whatever happens in between, so a failing test cannot leave work suspended or hang.
    /// </summary>
    private sealed class Gate
    {
        private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);
        private readonly TaskCompletionSource _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Called by the held operation: signals arrival and waits (bounded) for release.</summary>
        public Task Hold()
        {
            _reached.TrySetResult();
            return _released.Task.WaitAsync(Bound);
        }

        /// <summary>Lets the held operation go on (again: harmless).</summary>
        public void Release() => _released.TrySetResult();

        /// <summary>
        /// Waits for the operation to arrive, runs <paramref name="meanwhile"/> (bounded too), then always releases the
        /// held operation and drains both: nothing is left suspended, even when a step times out.
        /// </summary>
        public async Task Run(Task held, Func<Task> meanwhile)
        {
            Task? concurrent = null;
            try
            {
                await Task.WhenAny(_reached.Task, held).WaitAsync(Bound);
                Assert.True(_reached.Task.IsCompleted, "The operation finished without reaching the held point.");
                concurrent = meanwhile();
                await concurrent.WaitAsync(Bound);
            }
            finally
            {
                Release();
                await Drain(held);
                if (concurrent is not null) await Drain(concurrent);
            }
        }

        /// <summary>Waits a bounded time for a task; its outcome is the test's to assert.</summary>
        private static async Task Drain(Task task)
        {
            try { await task.WaitAsync(Bound); }
            catch (Exception) when (task.IsCompleted) { }
        }
    }

    [Fact]
    public void TheSourceSaysItsDeadLetterTopicIsShared()
    {
        var index = Build(["projects/p/subscriptions/dlq-reader", "projects/p/subscriptions/audit"]);

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

    private static GooglePubSubTopology Build(string[] readers, string[]? detached = null) =>
        GooglePubSubTopology.Build("p", ["events", "events-dlq", "billing-dlq"], Subscriptions(readers, detached), DateTimeOffset.UtcNow);

    private static Subscription[] Subscriptions(string[] readers, string[]? detached) =>
        [
            new Subscription { Name = "projects/p/subscriptions/orders", Topic = "projects/p/topics/events", DeadLetterPolicy = new DeadLetterPolicy { DeadLetterTopic = DeadLetterTopic, MaxDeliveryAttempts = 5 } },
            new Subscription { Name = "projects/p/subscriptions/billing", Topic = "projects/p/topics/events", DeadLetterPolicy = new DeadLetterPolicy { DeadLetterTopic = BillingTopic, MaxDeliveryAttempts = 5 } },
            new Subscription { Name = "projects/p/subscriptions/billing-dlq-reader", Topic = BillingTopic },
            .. readers.Where(reader => reader.StartsWith("projects/p/", StringComparison.Ordinal))
                .Select(reader => new Subscription { Name = reader, Topic = DeadLetterTopic }),
            .. (detached ?? []).Select(reader => new Subscription { Name = reader, Topic = DeadLetterTopic, Detached = true })
        ];

    private (GooglePubSubWorkspace Workspace, FakePublisher Publisher, FakeSubscriber Subscriber) Connect(
        string[][] pages, StatusCode? listFailure = null, string[]? detached = null)
    {
        // The cached topology knows this project's readers only.
        var index = Build(pages.SelectMany(page => page).ToArray(), detached);
        var publisher = new FakePublisher { Pages = pages, ListFailure = listFailure, Topics = ["events", "events-dlq", "billing-dlq"] };
        var subscriber = new FakeSubscriber { All = Subscriptions(pages.SelectMany(page => page).ToArray(), detached) };
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
        public StatusCode? ListFailure { get; set; }

        /// <summary>Readers listed for topics other than the main dead-letter topic.</summary>
        public Dictionary<string, string[]> OtherTopicReaders { get; } = new() { [BillingTopic] = ["projects/p/subscriptions/billing-dlq-reader"] };

        /// <summary>The project's topics, listed when a deletion refreshes the topology.</summary>
        public string[] Topics { get; init; } = [];

        public override PagedAsyncEnumerable<ListTopicsResponse, Topic> ListTopicsAsync(ListTopicsRequest request, CallSettings? callSettings = null) =>
            new OnePage<ListTopicsResponse, Topic>(new ListTopicsResponse { Topics = { Topics.Select(id => new Topic { Name = $"projects/p/topics/{id}" }) } }, page => page.Topics);
        public List<string> ListedTopics { get; } = [];
        public int Published { get; private set; }

        /// <summary>When set, awaited before a listing hands out its last page.</summary>
        public Func<Task>? BeforeLastPage { get; set; }

        public override PagedAsyncEnumerable<ListTopicSubscriptionsResponse, string> ListTopicSubscriptionsAsync(
            ListTopicSubscriptionsRequest request, CallSettings? callSettings = null)
        {
            ListedTopics.Add(request.Topic);
            if (ListFailure is { } failure) throw new RpcException(new Status(failure, "listing refused"));
            var pages = request.Topic == DeadLetterTopic ? Pages : [OtherTopicReaders.GetValueOrDefault(request.Topic, [])];
            return new FakePaged(pages.Select(names => new ListTopicSubscriptionsResponse { Subscriptions = { names } }).ToArray(), BeforeLastPage);
        }

        public override Task<PublishResponse> PublishAsync(PublishRequest request, CallSettings? callSettings = null)
        {
            Published += request.Messages.Count;
            return Task.FromResult(new PublishResponse { MessageIds = { request.Messages.Select(_ => "published") } });
        }
    }

    private sealed class OnePage<TResponse, TResource>(TResponse page, Func<TResponse, IEnumerable<TResource>> items) : PagedAsyncEnumerable<TResponse, TResource>
    {
        public override IAsyncEnumerable<TResponse> AsRawResponses() => Raw();

        public override Task<Page<TResource>> ReadPageAsync(int pageSize, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public override async IAsyncEnumerator<TResource> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            foreach (var item in items(page)) yield return item;
        }

        private async IAsyncEnumerable<TResponse> Raw()
        {
            await Task.Yield();
            yield return page;
        }
    }

    private sealed class FakePaged(ListTopicSubscriptionsResponse[] pages, Func<Task>? beforeLastPage) : PagedAsyncEnumerable<ListTopicSubscriptionsResponse, string>
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
            for (var index = 0; index < pages.Length; index++)
            {
                await Task.Yield();
                if (index == pages.Length - 1 && beforeLastPage is { } gate) await gate();
                yield return pages[index];
            }
        }
    }

    private sealed class FakeSubscriber : SubscriberServiceApiClient
    {
        private bool _delivered;
        public int Pulls { get; private set; }
        public List<string> AcknowledgedIds { get; } = [];
        public List<string> ReleasedIds { get; } = [];

        /// <summary>The dead-letter topic the service reports for each subscription now.</summary>
        public Dictionary<string, string> Policies { get; } = new() { ["orders"] = DeadLetterTopic, ["billing"] = BillingTopic };

        /// <summary>The project's subscriptions, listed when a deletion refreshes the topology.</summary>
        public Subscription[] All { get; set; } = [];

        public override PagedAsyncEnumerable<ListSubscriptionsResponse, Subscription> ListSubscriptionsAsync(ListSubscriptionsRequest request, CallSettings? callSettings = null) =>
            new OnePage<ListSubscriptionsResponse, Subscription>(new ListSubscriptionsResponse { Subscriptions = { All } }, page => page.Subscriptions);

        /// <summary>When set, awaited (with the subscription's id) before a subscription is read.</summary>
        public Func<string, Task>? BeforeGet { get; set; }

        public override async Task<Subscription> GetSubscriptionAsync(GetSubscriptionRequest request, CallSettings? callSettings = null)
        {
            var id = request.SubscriptionAsSubscriptionName.SubscriptionId;
            if (BeforeGet is { } gate) await gate(id);
            return await (Policies.TryGetValue(id, out var topic)
                ? Task.FromResult(new Subscription { Name = request.Subscription, DeadLetterPolicy = new DeadLetterPolicy { DeadLetterTopic = topic, MaxDeliveryAttempts = 5 } })
                : Task.FromException<Subscription>(new RpcException(new Status(StatusCode.NotFound, "no such subscription"))));
        }

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
