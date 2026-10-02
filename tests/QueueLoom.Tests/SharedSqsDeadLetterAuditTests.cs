using System.Reflection;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class SharedSqsDeadLetterAuditTests
{
    private const string QueueArn = "arn:aws:sqs:us-east-1:123:q";
    private const string SharedArn = "arn:aws:sqs:us-east-1:123:shared";

    [Fact]
    public async Task QueueScopedBrowseReturnsOnlyExactPositiveSourceAttribution()
    {
        using var directory = new TemporaryDirectory();
        var broker = new MixedSqsClient();
        await using var workspace = CreateWorkspace(directory.Path, broker);
        var messages = await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue("q"), ServiceBusSubQueue.DeadLetter));
        Assert.Equal(["q-message"], messages.Select(m => m.Properties.MessageId));
        Assert.Equal(broker.Ids.Order(), broker.Released.Order());
        Assert.Empty(broker.Deleted);
    }

    [Fact]
    public async Task QueueScopedPurgeNeverBacksUpOrSettlesUnmarkedSnsFailures()
    {
        using var directory = new TemporaryDirectory();
        var broker = new MixedSqsClient();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        await using var workspace = CreateWorkspace(directory.Path, broker);
        var result = await workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest(
            [ServiceBusEntityReference.Queue("q")], [ServiceBusSubQueue.DeadLetter]));
        Assert.False(result.HasFailures);
        Assert.Equal(1, result.DeletedCount);
        Assert.Equal(["q-message"], broker.Deleted);
        Assert.Equal(broker.Ids.Where(id => id != "q-message").Order(), broker.Released.Order());
        var backup = Assert.Single(await new JsonDeadLetterBackupRepository(paths).ListAsync());
        Assert.Equal("q-message", backup.MessageId);
        Assert.Equal(ServiceBusEntityReference.Queue("q"), backup.Source);
    }

    [Fact]
    public async Task QueueScopedSelectiveDeleteCannotSelectAnUnmarkedSnsFailure()
    {
        using var directory = new TemporaryDirectory();
        var broker = new MixedSqsClient();
        await using var workspace = CreateWorkspace(directory.Path, broker);
        var source = ServiceBusEntityReference.Queue("q");
        var result = await workspace.DeleteDeadLetterMessagesAsync(new DeleteDeadLetterMessagesRequest(
            [new DeadLetterMessageKey(source, ServiceBusSubQueue.DeadLetter, LeasedMessageIdentity.SequenceNumberFor("sns-message"), "sns-message")]));
        Assert.Equal(0, result.DeletedCount);
        Assert.Equal(1, result.NotFoundCount);
        Assert.Empty(broker.Deleted);
        Assert.Equal(broker.Ids.Order(), broker.Released.Order());
        Assert.Empty(await new JsonDeadLetterBackupRepository(QueueLoomPaths.ForRoot(directory.Path)).ListAsync());
    }

    [Fact]
    public async Task ExplicitPhysicalQueueBrowseKeepsAllDeliveriesWithoutAttributingTheirOrigin()
    {
        using var directory = new TemporaryDirectory();
        var broker = new MixedSqsClient();
        await using var workspace = CreateWorkspace(directory.Path, broker);
        var messages = await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue("shared"), ServiceBusSubQueue.Active));
        Assert.Equal(broker.Ids, messages.Select(m => m.Properties.MessageId));
        Assert.All(messages, message => Assert.Equal(ServiceBusEntityReference.Queue("shared"), message.Source));
        Assert.Equal(broker.Ids.Order(), broker.Released.Order());
        Assert.Empty(broker.Deleted);
    }

    private static AwsSqsSnsWorkspace CreateWorkspace(string root, MixedSqsClient broker)
    {
        var owner = new AwsSqsSnsWorkspace(new DeepAuditCloudTests.EmptyVault(), backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(root)));
        var shared = new AwsQueueInfo("shared", "http://localhost/shared", SharedArn, false, broker.Ids.Length, 0, 0, null, null, null);
        var queue = new AwsQueueInfo("q", "http://localhost/q", QueueArn, false, 0, 0, 0, SharedArn, null, null);
        var subscription = new AwsSubscriptionInfo("arn:aws:sns:us-east-1:123:events:s", "sqs", QueueArn, SharedArn) { Name = "s" };
        var index = new AwsTopologyIndex([shared, queue], [new AwsTopicInfo("events", "arn:aws:sns:us-east-1:123:events", false, [subscription])]);
        var profile = ViewModelStateTests.CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite) with { Provider = MessagingProvider.AmazonSqsSns };
        Set(typeof(AwsSqsSnsWorkspace), "_index", index); Set(typeof(AwsSqsSnsWorkspace), "_sqs", broker);
        Set(typeof(LeasedMessagingWorkspace), "_profile", profile);
        Set(typeof(LeasedMessagingWorkspace), "_connectionState", WorkspaceConnectionState.Connected);
        Set(typeof(LeasedMessagingWorkspace), "_cachedTopology", index.ToTopology(DateTimeOffset.UtcNow));
        return owner;
        void Set(Type type, string field, object value) => type.GetField(field, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner, value);
    }

    private sealed class MixedSqsClient() : AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig { ServiceURL = "http://localhost" })
    {
        private bool _received;
        public string[] Ids { get; } = ["q-message", "sns-message", "empty-source", "other-queue", "wrong-case"];
        public List<string> Released { get; } = [];
        public List<string> Deleted { get; } = [];
        public override Task<ReceiveMessageResponse> ReceiveMessageAsync(ReceiveMessageRequest request, CancellationToken cancellationToken = default)
        {
            Assert.Contains("All", request.MessageSystemAttributeNames);
            if (_received) return Task.FromResult(new ReceiveMessageResponse { Messages = [] });
            _received = true;
            string?[] arns = [QueueArn, null, "", "arn:aws:sqs:us-east-1:123:other", "arn:aws:sqs:us-east-1:123:Q"];
            return Task.FromResult(new ReceiveMessageResponse
            {
                Messages = Ids.Select((id, i) => new Message
                {
                    MessageId = id, ReceiptHandle = id, Body = id,
                    Attributes = arns[i] is { } arn ? new() { ["DeadLetterQueueSourceArn"] = arn } : new()
                }).ToList()
            });
        }
        public override Task<DeleteMessageBatchResponse> DeleteMessageBatchAsync(DeleteMessageBatchRequest request, CancellationToken cancellationToken = default)
        { Deleted.AddRange(request.Entries.Select(e => e.ReceiptHandle)); return Task.FromResult(new DeleteMessageBatchResponse { Failed = [] }); }
        public override Task<ChangeMessageVisibilityBatchResponse> ChangeMessageVisibilityBatchAsync(ChangeMessageVisibilityBatchRequest request, CancellationToken cancellationToken = default)
        { Released.AddRange(request.Entries.Select(e => e.ReceiptHandle)); return Task.FromResult(new ChangeMessageVisibilityBatchResponse { Failed = [] }); }
    }
}
