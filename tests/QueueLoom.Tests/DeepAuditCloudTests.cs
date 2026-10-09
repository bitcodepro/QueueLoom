using System.Reflection;
using Google.Api.Gax.Grpc;
using Amazon.SQS.Model;
using Google.Cloud.PubSub.V1;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Google;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Tests;

public sealed class DeepAuditCloudTests
{
    [Theory]
    [InlineData("events", "projects/project-a/topics/events")]
    [InlineData("projects/project-b/topics/events", "projects/project-b/topics/events")]
    public async Task PubSubPublishesToTheApprovedCanonicalTopic(string destination, string expected)
    {
        await using var owner = new GooglePubSubWorkspace(new EmptyVault());
        var publisher = new CapturingPublisher();
        typeof(GooglePubSubWorkspace).GetField("_publisher", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner, publisher);
        typeof(GooglePubSubWorkspace).GetField("_projectId", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner, "project-a");
        var task = (Task)typeof(GooglePubSubWorkspace).GetMethod("SendCoreAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(owner, [new ServiceBusTopology(DateTimeOffset.UtcNow), ServiceBusEntityReference.Topic(destination), new MessageDraft(new EditableMessageBody("x", MessageBodyFormat.Text)), CancellationToken.None])!;
        await task;
        Assert.Equal(expected, publisher.PublishedTopic);
    }

    private sealed class CapturingPublisher : PublisherServiceApiClient
    {
        public string? PublishedTopic { get; private set; }
        public override Task<PublishResponse> PublishAsync(PublishRequest request, CallSettings? callSettings = null)
        { PublishedTopic = request.Topic; return Task.FromResult(new PublishResponse()); }
    }

    [Fact]
    public async Task SnsPurgeAndSelectiveDeleteAreBlockedBeforeAnyBackupOrSettlement()
    {
        using var directory = new QueueLoom.Tests.Infrastructure.TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        await using var owner = new AwsSqsSnsWorkspace(new EmptyVault(), backupStore: new DeadLetterJsonBackupStore(paths));
        const string dlqArn = "arn:aws:sqs:us-east-1:123:shared";
        var dlq = new AwsQueueInfo("shared", "http://localhost/shared", dlqArn, false, 2, 0, 0, null, null, null);
        var queue = new AwsQueueInfo("q", "http://localhost/q", "arn:aws:sqs:us-east-1:123:q", false, 0, 0, 0, dlqArn, null, null);
        var subscription = new AwsSubscriptionInfo("arn:aws:sns:us-east-1:123:events:s", "sqs", queue.Arn, dlqArn) { Name = "s" };
        var index = new AwsTopologyIndex([dlq, queue], [new AwsTopicInfo("events", "arn:aws:sns:us-east-1:123:events", false, [subscription])]);
        var profile = ViewModelStateTests.CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite) with { Provider = MessagingProvider.AmazonSqsSns };
        typeof(AwsSqsSnsWorkspace).GetField("_index", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner, index);
        typeof(LeasedMessagingWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner, profile);
        typeof(LeasedMessagingWorkspace).GetField("_connectionState", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner, WorkspaceConnectionState.Connected);
        typeof(LeasedMessagingWorkspace).GetField("_cachedTopology", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner, index.ToTopology(DateTimeOffset.UtcNow));
        var source = ServiceBusEntityReference.Subscription("events", "s");
        // No SQS client is supplied: rejection must occur before receiving or settling either Q's message
        // or an unmarked SNS delivery in the shared physical queue.
        var purgeError = await Assert.ThrowsAsync<InvalidOperationException>(() => owner.PurgeDeadLettersAsync(
            new DeadLetterPurgeRequest([source], [ServiceBusSubQueue.DeadLetter])));
        Assert.Contains("Source-scoped deletion is blocked", purgeError.Message, StringComparison.Ordinal);
        var deleteError = await Assert.ThrowsAsync<InvalidOperationException>(() => owner.DeleteDeadLetterMessagesAsync(
            new DeleteDeadLetterMessagesRequest([new DeadLetterMessageKey(source, ServiceBusSubQueue.DeadLetter, 1, "q-message")])));
        Assert.Contains("Source-scoped deletion is blocked", deleteError.Message, StringComparison.Ordinal);
        Assert.False(Directory.Exists(paths.BackupsDirectory));
    }
    [Fact]
    public void PubSubKeepsLocalAndForeignTopicsWithTheSameIdApart()
    {
        var local = new Subscription { Name = "projects/project-a/subscriptions/local", Topic = "projects/project-a/topics/events" };
        var foreign = new Subscription { Name = "projects/project-a/subscriptions/foreign", Topic = "projects/project-b/topics/events" };
        var index = GooglePubSubTopology.Build("project-a", ["events"], [local, foreign], DateTimeOffset.UtcNow);
        Assert.Equal(2, index.Topology.Topics.Count);
        var foreignSource = index.Topology.Topics.SelectMany(t => t.Subscriptions).Single(s => s.Name == "foreign").Reference;
        Assert.Equal("projects/project-b/topics/events", DeadLetterResender.OriginalDestination(foreignSource).Name);
        Assert.Single(index.Topology.Topics.Single(t => t.Name == "events").Subscriptions);
    }

    [Theory]
    [InlineData("billing", "project-a", true)]
    [InlineData("billing", "project-b", false)]
    [InlineData("billing", null, false)]
    [InlineData("projects/project-a/subscriptions/billing", null, true)]
    [InlineData("projects/project-b/subscriptions/billing", "project-a", false)]
    [InlineData("projects/project-a/subscriptions/billing", "project-b", false)]
    [InlineData(null, null, false)]
    public void PubSubDeadLettersRequireTheCanonicalSourceProject(string? subscription, string? project, bool expected)
    {
        var owner = new GooglePubSubWorkspace(new EmptyVault());
        var type = typeof(GooglePubSubWorkspace).GetNestedType("PubSubChannel", BindingFlags.NonPublic)!;
        var channel = Activator.CreateInstance(type, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null,
            [owner, ServiceBusEntityReference.Subscription("events", "billing"), ServiceBusSubQueue.DeadLetter,
                new SubscriptionName("project-a", "reader"), new SubscriptionName("project-a", "billing")], null)!;
        var message = new PubsubMessage();
        if (subscription is not null) message.Attributes[GooglePubSubWorkspace.DeadLetterSourceSubscription] = subscription;
        if (project is not null) message.Attributes["CloudPubSubDeadLetterSourceSubscriptionProject"] = project;
        var belongs = (bool)type.GetMethod("BelongsToSource", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(channel, [message])!;
        Assert.Equal(expected, belongs);
    }

    [Fact]
    public void SnsSharedDeadLetterQueueNeverClaimsAnSqsMessage()
    {
        var owner = new AwsSqsSnsWorkspace(new EmptyVault());
        const string dlqArn = "arn:aws:sqs:us-east-1:123:shared";
        var dlq = new AwsQueueInfo("shared", "http://localhost/shared", dlqArn, false, 2, 0, 0, null, null, null);
        var queue = new AwsQueueInfo("q", "http://localhost/q", "arn:aws:sqs:us-east-1:123:q", false, 0, 0, 0, dlqArn, null, null);
        var subscription = new AwsSubscriptionInfo("arn:aws:sns:us-east-1:123:events:s", "sqs", queue.Arn, dlqArn) { Name = "s" };
        var index = new AwsTopologyIndex([dlq, queue], [new AwsTopicInfo("events", "arn:aws:sns:us-east-1:123:events", false, [subscription])]);
        typeof(AwsSqsSnsWorkspace).GetField("_index", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner, index);
        var channel = (ILeasedMessageChannel)typeof(AwsSqsSnsWorkspace).GetMethod("OpenChannel", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(owner, [index.ToTopology(DateTimeOffset.UtcNow), ServiceBusEntityReference.Subscription("events", "s"), ServiceBusSubQueue.DeadLetter])!;
        var message = new Message { MessageId = "q-message", Body = "q-body", Attributes = new() { ["DeadLetterQueueSourceArn"] = queue.Arn } };
        var belongs = (bool)channel.GetType().GetMethod("BelongsToSource", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(channel, [message])!;
        Assert.False(belongs);
        // SNS's unmarked deliveries cannot be assigned to this particular subscription either.
        message.Attributes.Clear();
        Assert.False((bool)channel.GetType().GetMethod("BelongsToSource", BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(channel, [message])!);
    }

    internal sealed class EmptyVault : ISecretVault
    {
        public ValueTask StoreAsync(ProfileSecretKey key, string secret, CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public ValueTask<string?> RetrieveAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) => ValueTask.FromResult<string?>(null);
        public ValueTask<bool> ExistsAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) => ValueTask.FromResult(false);
        public ValueTask<bool> RemoveAsync(ProfileSecretKey key, CancellationToken cancellationToken = default) => ValueTask.FromResult(false);
    }
}
