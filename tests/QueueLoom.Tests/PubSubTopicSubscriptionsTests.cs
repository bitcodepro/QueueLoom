using System.Reflection;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Google;
using QueueLoom.Infrastructure.Messaging;

namespace QueueLoom.Tests;

public sealed class PubSubTopicSubscriptionsTests
{
    // projects.topics.subscriptions.list "Lists the names of the attached subscriptions on this topic", whichever
    // project they live in; the project listing used before only saw this project's subscriptions.
    [Fact]
    public async Task Rules_CountSubscriptionsInOtherProjectsAcrossPages()
    {
        var publisher = new FakePublisher
        {
            TopicSubscriptions =
            [
                ["projects/project-b/subscriptions/audit"],
                ["projects/project-c/subscriptions/secret", "projects/project-a/subscriptions/local"]
            ]
        };
        var subscriber = new FakeSubscriber();
        subscriber.Existing["projects/project-b/subscriptions/audit"] = new Subscription
        {
            Name = "projects/project-b/subscriptions/audit", Topic = "projects/project-a/topics/events", Filter = "attributes.region = \"EU\""
        };
        subscriber.Existing["projects/project-a/subscriptions/local"] = new Subscription
        {
            Name = "projects/project-a/subscriptions/local", Topic = "projects/project-a/topics/events"
        };
        subscriber.Denied.Add("projects/project-c/subscriptions/secret");
        await using var workspace = Workspace(publisher, subscriber);

        var rules = await workspace.GetTopicRulesAsync("events");

        Assert.Equal("projects/project-a/topics/events", publisher.ListedTopic);
        Assert.Equal(2, publisher.PagesRead);
        Assert.Equal(["local", "projects/project-b/subscriptions/audit", "projects/project-c/subscriptions/secret"],
            rules.Select(item => item.Subscription));
        var audit = rules.Single(item => item.Subscription.EndsWith("/audit", StringComparison.Ordinal));
        Assert.Equal("attributes.region = \"EU\"", Assert.Single(audit.Rules).Expression);
        Assert.Contains("project-b", audit.Note, StringComparison.Ordinal);
        var secret = rules.Single(item => item.Subscription.EndsWith("/secret", StringComparison.Ordinal));
        Assert.NotNull(secret.Unreadable);

        var message = new RoutingMessage(new EditableMessageProperties(), [new MessageApplicationProperty("region", ApplicationPropertyType.String, "US")]);
        var routed = TopicRouting.Route("events", rules, message, RoutingService.PubSub);
        Assert.False(routed.IsDropped);
        Assert.DoesNotContain("has no subscriptions", routed.Headline, StringComparison.Ordinal);
        Assert.Equal(RoutingOutcome.Unknown, routed.Subscriptions.Single(item => item.Subscription.EndsWith("/secret", StringComparison.Ordinal)).Outcome);
        Assert.Equal(RoutingOutcome.Skips, routed.Subscriptions.Single(item => item.Subscription.EndsWith("/audit", StringComparison.Ordinal)).Outcome);
    }

    [Fact]
    public async Task Rules_TopicWhoseOnlySubscriptionIsElsewhereIsNotReportedAsDroppingEverything()
    {
        var publisher = new FakePublisher { TopicSubscriptions = [["projects/project-b/subscriptions/audit"]] };
        var subscriber = new FakeSubscriber();
        subscriber.Existing["projects/project-b/subscriptions/audit"] = new Subscription
        {
            Name = "projects/project-b/subscriptions/audit", Topic = "projects/project-a/topics/events"
        };
        await using var workspace = Workspace(publisher, subscriber);

        var rules = await workspace.GetTopicRulesAsync("events");

        var routed = TopicRouting.Route("events", rules, new RoutingMessage(new EditableMessageProperties(), Array.Empty<MessageApplicationProperty>()), RoutingService.PubSub);
        Assert.Single(routed.Subscriptions);
        Assert.Equal(RoutingOutcome.Receives, routed.Subscriptions[0].Outcome);
        Assert.Equal("1 of 1 subscriptions receive it.", routed.Headline);
    }

    // projects.subscriptions.create "returns ALREADY_EXISTS" for a taken name, but the dead-letter topic and its
    // subscription were made first and stayed behind. A taken name is now refused before anything is created.
    [Fact]
    public async Task Create_TakenNameIsRefusedBeforeTheDeadLetterTopicIsCreated()
    {
        var publisher = new FakePublisher();
        var subscriber = new FakeSubscriber();
        subscriber.Existing["projects/project-a/subscriptions/orders"] = new Subscription { Name = "projects/project-a/subscriptions/orders" };
        await using var workspace = Workspace(publisher, subscriber);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.CreateQueueAsync(
            new QueueDefinition("orders", new QueueSettings(), CreateDeadLetterQueue: true, TopicName: "events")));

        Assert.Contains("already exists", error.Message, StringComparison.Ordinal);
        Assert.Empty(publisher.CreatedTopics);
        Assert.Empty(subscriber.Created);
    }

    // When the main subscription is refused or taken after the check (another operator in the same moment), the
    // dead-letter topic and subscription are not deleted: that operator may already use them. They are named.
    [Theory]
    [InlineData(StatusCode.AlreadyExists)]
    [InlineData(StatusCode.PermissionDenied)]
    public async Task Create_RefusedMainSubscriptionNamesWhatWasCreatedAndDeletesNothing(StatusCode refusal)
    {
        var publisher = new FakePublisher();
        var subscriber = new FakeSubscriber { Refuse = ("projects/project-a/subscriptions/orders", refusal) };
        await using var workspace = Workspace(publisher, subscriber);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.CreateQueueAsync(
            new QueueDefinition("orders", new QueueSettings(), CreateDeadLetterQueue: true, TopicName: "events")));

        Assert.Contains("projects/project-a/topics/orders-dead-letter", error.Message, StringComparison.Ordinal);
        Assert.Contains("projects/project-a/subscriptions/orders-dead-letter", error.Message, StringComparison.Ordinal);
        Assert.Contains("left in place", error.Message, StringComparison.Ordinal);
        Assert.Equal(["projects/project-a/topics/orders-dead-letter"], publisher.CreatedTopics);
        Assert.Empty(publisher.DeletedTopics);
        Assert.Empty(subscriber.Deleted);
    }

    // The subscription is created, but the response is lost (DEADLINE_EXCEEDED): the read-back finds it with the
    // requested topic, so the create succeeds instead of being reported as a definite refusal.
    [Fact]
    public async Task Create_AppliedButTimedOutIsReconciledAsCreated()
    {
        var publisher = new FakePublisher();
        var subscriber = new FakeSubscriber { LoseResponseOf = "projects/project-a/subscriptions/orders" };
        await using var workspace = Workspace(publisher, subscriber);

        await workspace.CreateQueueAsync(new QueueDefinition("orders", new QueueSettings(), CreateDeadLetterQueue: true, TopicName: "events"));

        Assert.Contains("projects/project-a/subscriptions/orders", subscriber.Created);
        Assert.Empty(subscriber.Deleted);
    }

    // The same name was created meanwhile on the same topic but without our settings, and our request then failed
    // ambiguously: that subscription is not the one requested, so creation is not reported as done.
    [Fact]
    public async Task Create_TimedOutWhileAnIncompatibleSameTopicSubscriptionAppearedIsNotSuccess()
    {
        var publisher = new FakePublisher();
        var subscriber = new FakeSubscriber
        {
            UnavailableOn = "projects/project-a/subscriptions/orders",
            AppearOnFailure = new Subscription { Name = "projects/project-a/subscriptions/orders", Topic = "projects/project-a/topics/events", AckDeadlineSeconds = 10 }
        };
        await using var workspace = Workspace(publisher, subscriber);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.CreateQueueAsync(new QueueDefinition("orders",
            new QueueSettings(LockDuration: TimeSpan.FromSeconds(60)), CreateDeadLetterQueue: true, TopicName: "events")));

        Assert.Contains("someone else", error.Message, StringComparison.Ordinal);
        Assert.Equal(10, subscriber.Existing["projects/project-a/subscriptions/orders"].AckDeadlineSeconds);
        Assert.Empty(subscriber.Deleted);
    }

    // A same-name, same-topic subscription with matching settings but a filter, a push endpoint or an export receives
    // different messages or delivers them elsewhere: it is not the ordinary subscription requested.
    [Theory]
    [InlineData("filter")]
    [InlineData("push")]
    [InlineData("bigquery")]
    [InlineData("ordering")]
    public async Task Create_TimedOutWhileASameTopicSubscriptionWithOtherDeliveryAppearedIsNotSuccess(string variant)
    {
        var appeared = new Subscription
        {
            Name = "projects/project-a/subscriptions/orders", Topic = "projects/project-a/topics/events",
            DeadLetterPolicy = new DeadLetterPolicy { DeadLetterTopic = "projects/project-a/topics/orders-dead-letter", MaxDeliveryAttempts = 5 }
        };
        switch (variant)
        {
            case "filter": appeared.Filter = "attributes.region = \"EU\""; break;
            case "push": appeared.PushConfig = new PushConfig { PushEndpoint = "https://example.test/push" }; break;
            case "bigquery": appeared.BigqueryConfig = new BigQueryConfig { Table = "p.d.t" }; break;
            case "ordering": appeared.EnableMessageOrdering = true; break;
        }
        var publisher = new FakePublisher();
        var subscriber = new FakeSubscriber { UnavailableOn = "projects/project-a/subscriptions/orders", AppearOnFailure = appeared };
        await using var workspace = Workspace(publisher, subscriber);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.CreateQueueAsync(
            new QueueDefinition("orders", new QueueSettings(), CreateDeadLetterQueue: true, TopicName: "events")));

        Assert.Contains("someone else", error.Message, StringComparison.Ordinal);
        Assert.Empty(subscriber.Deleted);
    }

    // The same subscription with the full requested configuration (as Pub/Sub returns it: an empty push config) is ours.
    [Fact]
    public async Task Create_TimedOutWhileTheRequestedSubscriptionAppearedIsSuccess()
    {
        var appeared = new Subscription
        {
            Name = "projects/project-a/subscriptions/orders", Topic = "projects/project-a/topics/events", PushConfig = new PushConfig(),
            DeadLetterPolicy = new DeadLetterPolicy { DeadLetterTopic = "projects/project-a/topics/orders-dead-letter", MaxDeliveryAttempts = 5 }
        };
        var subscriber = new FakeSubscriber { UnavailableOn = "projects/project-a/subscriptions/orders", AppearOnFailure = appeared };
        await using var workspace = Workspace(new FakePublisher(), subscriber);

        await workspace.CreateQueueAsync(new QueueDefinition("orders", new QueueSettings(), CreateDeadLetterQueue: true, TopicName: "events"));
    }

    // Dead-letter setup failed before the subscription itself was requested; a same-topic subscription created by someone
    // else meanwhile must not be taken as ours.
    [Fact]
    public async Task Create_FailingInDeadLetterSetupIsNotSuccessEvenIfTheNameAppeared()
    {
        var publisher = new FakePublisher();
        var subscriber = new FakeSubscriber
        {
            UnavailableOn = "projects/project-a/subscriptions/orders-dead-letter",
            AppearOnFailure = new Subscription { Name = "projects/project-a/subscriptions/orders", Topic = "projects/project-a/topics/events" }
        };
        await using var workspace = Workspace(publisher, subscriber);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.CreateQueueAsync(
            new QueueDefinition("orders", new QueueSettings(), CreateDeadLetterQueue: true, TopicName: "events")));

        Assert.Contains("did not create", error.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("projects/project-a/subscriptions/orders", subscriber.Created);
        Assert.Empty(subscriber.Deleted);
    }

    // A timeout whose outcome cannot be read back is reported as uncertain: the dead-letter resources may already serve
    // the subscription, so the message must not present them as safe to delete.
    [Fact]
    public async Task Create_TimedOutAndUnreadableIsReportedAsUncertain()
    {
        var publisher = new FakePublisher();
        var subscriber = new FakeSubscriber { LoseResponseOf = "projects/project-a/subscriptions/orders", DenyReadsAfterLoss = true };
        await using var workspace = Workspace(publisher, subscriber);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => workspace.CreateQueueAsync(
            new QueueDefinition("orders", new QueueSettings(), CreateDeadLetterQueue: true, TopicName: "events")));

        Assert.Contains("unknown", error.Message, StringComparison.Ordinal);
        Assert.Contains("before deleting anything", error.Message, StringComparison.Ordinal);
        Assert.Empty(subscriber.Deleted);
    }

    // A subscription listed for topic A was deleted and recreated under the same name for topic B before it was read:
    // it no longer receives A's messages and must not be shown as one of A's recipients.
    [Fact]
    public async Task Rules_SkipASubscriptionRecreatedForAnotherTopicMeanwhile()
    {
        var publisher = new FakePublisher { TopicSubscriptions = [["projects/project-a/subscriptions/moved"]] };
        var subscriber = new FakeSubscriber();
        subscriber.Existing["projects/project-a/subscriptions/moved"] = new Subscription
        {
            Name = "projects/project-a/subscriptions/moved", Topic = "projects/project-a/topics/other"
        };
        await using var workspace = Workspace(publisher, subscriber);

        var rules = await workspace.GetTopicRulesAsync("events");

        Assert.DoesNotContain(rules, item => item.Subscription == "moved");
    }

    private static GooglePubSubWorkspace Workspace(FakePublisher publisher, FakeSubscriber subscriber)
    {
        var workspace = new GooglePubSubWorkspace(new DeepAuditCloudTests.EmptyVault());
        Set(typeof(GooglePubSubWorkspace), workspace, "_publisher", publisher);
        Set(typeof(GooglePubSubWorkspace), workspace, "_subscriber", subscriber);
        Set(typeof(GooglePubSubWorkspace), workspace, "_projectId", "project-a");
        Set(typeof(LeasedMessagingWorkspace), workspace, "_profile",
            ViewModelStateTests.CreateProfile("Isolated", EnvironmentKind.Test, ProfileAccessMode.ReadWrite) with
            {
                Provider = MessagingProvider.GooglePubSub, AllowQueueManagement = true
            });
        return workspace;
    }

    private static void Set(System.Type type, object target, string name, object value) =>
        type.GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(target, value);

    private sealed class FakePublisher : PublisherServiceApiClient
    {
        public string[][] TopicSubscriptions { get; init; } = [];
        public string? ListedTopic { get; private set; }
        public int PagesRead { get; private set; }
        public List<string> CreatedTopics { get; } = [];
        public List<string> DeletedTopics { get; } = [];

        public override PagedAsyncEnumerable<ListTopicSubscriptionsResponse, string> ListTopicSubscriptionsAsync(
            ListTopicSubscriptionsRequest request, CallSettings? callSettings = null)
        {
            ListedTopic = request.Topic;
            var pages = TopicSubscriptions.Select((names, index) => new ListTopicSubscriptionsResponse
            {
                Subscriptions = { names },
                NextPageToken = index < TopicSubscriptions.Length - 1 ? $"page-{index + 1}" : string.Empty
            }).ToArray();
            return new FakePaged<ListTopicSubscriptionsResponse, string>(pages, page => page.Subscriptions, () => PagesRead++);
        }

        public override Task<Topic> CreateTopicAsync(Topic request, CallSettings? callSettings = null)
        {
            CreatedTopics.Add(request.Name);
            return Task.FromResult(request);
        }

        public override Task DeleteTopicAsync(DeleteTopicRequest request, CallSettings? callSettings = null)
        {
            DeletedTopics.Add(request.Topic);
            return Task.CompletedTask;
        }
    }

    private sealed class FakeSubscriber : SubscriberServiceApiClient
    {
        public Dictionary<string, Subscription> Existing { get; } = new(StringComparer.Ordinal);
        public HashSet<string> Denied { get; } = new(StringComparer.Ordinal);
        public (string Name, StatusCode Status)? Refuse { get; init; }
        /// <summary>Created on the server, then DEADLINE_EXCEEDED before the response arrives.</summary>
        public string? LoseResponseOf { get; init; }
        public bool DenyReadsAfterLoss { get; init; }
        /// <summary>Creating this name fails with Unavailable after <see cref="AppearOnFailure"/> was created by someone else.</summary>
        public string? UnavailableOn { get; init; }
        public Subscription? AppearOnFailure { get; init; }
        private bool _lost;
        public List<string> Created { get; } = [];
        public List<string> Deleted { get; } = [];

        public override Task<Subscription> GetSubscriptionAsync(GetSubscriptionRequest request, CallSettings? callSettings = null) =>
            _lost && DenyReadsAfterLoss ? Task.FromException<Subscription>(new RpcException(new Status(StatusCode.Unavailable, "unavailable")))
            : Denied.Contains(request.Subscription) ? Task.FromException<Subscription>(new RpcException(new Status(StatusCode.PermissionDenied, "denied")))
            : Existing.TryGetValue(request.Subscription, out var subscription) ? Task.FromResult(subscription)
            : Task.FromException<Subscription>(new RpcException(new Status(StatusCode.NotFound, "not found")));

        public override Task<Subscription> CreateSubscriptionAsync(Subscription request, CallSettings? callSettings = null)
        {
            if (Refuse is { } refuse && refuse.Name == request.Name)
                return Task.FromException<Subscription>(new RpcException(new Status(refuse.Status, "refused by the fake")));
            if (request.Name == UnavailableOn)
            {
                if (AppearOnFailure is { } other) Existing[other.Name] = other;
                return Task.FromException<Subscription>(new RpcException(new Status(StatusCode.Unavailable, "unavailable")));
            }
            if (Existing.ContainsKey(request.Name))
                return Task.FromException<Subscription>(new RpcException(new Status(StatusCode.AlreadyExists, "exists")));
            Created.Add(request.Name);
            Existing[request.Name] = request;
            if (request.Name == LoseResponseOf)
            {
                _lost = true;
                return Task.FromException<Subscription>(new RpcException(new Status(StatusCode.DeadlineExceeded, "deadline exceeded")));
            }
            return Task.FromResult(request);
        }

        public override Task DeleteSubscriptionAsync(DeleteSubscriptionRequest request, CallSettings? callSettings = null)
        {
            Deleted.Add(request.Subscription);
            return Task.CompletedTask;
        }

        public override PagedAsyncEnumerable<ListSubscriptionsResponse, Subscription> ListSubscriptionsAsync(
            ListSubscriptionsRequest request, CallSettings? callSettings = null) =>
            new FakePaged<ListSubscriptionsResponse, Subscription>(
                [new ListSubscriptionsResponse { Subscriptions = { Existing.Values.Where(item => item.Name.StartsWith(request.Project + "/", StringComparison.Ordinal)) } }],
                page => page.Subscriptions, () => { });
    }

    /// <summary>The library's paged result over fixed pages, read page by page as the client library does.</summary>
    private sealed class FakePaged<TResponse, TResource>(TResponse[] pages, Func<TResponse, IEnumerable<TResource>> items, Action pageRead)
        : PagedAsyncEnumerable<TResponse, TResource>
    {
        public override IAsyncEnumerable<TResponse> AsRawResponses() => Raw();

        public override Task<Page<TResource>> ReadPageAsync(int pageSize, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public override async IAsyncEnumerator<TResource> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            await foreach (var page in Raw().WithCancellation(cancellationToken))
            {
                foreach (var item in items(page))
                {
                    yield return item;
                }
            }
        }

        private async IAsyncEnumerable<TResponse> Raw()
        {
            foreach (var page in pages)
            {
                await Task.Yield();
                pageRead();
                yield return page;
            }
        }
    }
}
