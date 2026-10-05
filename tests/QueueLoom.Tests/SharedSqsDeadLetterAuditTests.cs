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

    [Fact]
    public async Task QueueScopedPurgeStopsWhenOtherSourcesNeverRunOut()
    {
        using var directory = new TemporaryDirectory();
        var broker = new EndlessOtherSourceClient();
        await using var workspace = CreateWorkspace(directory.Path, broker, 0);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        var result = await workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest(
            [ServiceBusEntityReference.Queue("q")], [ServiceBusSubQueue.DeadLetter]), timeout.Token);

        Assert.False(timeout.IsCancellationRequested);
        var source = Assert.Single(result.Sources);
        Assert.Contains("other sources", source.Error, StringComparison.Ordinal);
        Assert.Equal(0, result.DeletedCount);
        Assert.Equal(LeasedMessagingWorkspace.MaximumHeldDuringPurge, broker.Released.Count);
    }

    // Review: a browse whose messages cannot all be made visible again still returns them, and the workspace tells
    // (CleanupWarning) that some stay hidden until their visibility timeout ends.
    [Fact]
    public async Task ABrowseWhoseReleaseFailsReturnsTheMessagesAndWarns()
    {
        using var directory = new TemporaryDirectory();
        var broker = new StuckReleaseClient();
        await using var workspace = CreateWorkspace(directory.Path, broker, 2);
        var warnings = new List<string>();
        ((ICleanupWarningSource)workspace).CleanupWarning += (_, warning) => warnings.Add(warning);

        var messages = await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue("q"), ServiceBusSubQueue.DeadLetter));

        Assert.Equal(["a", "b"], messages.Select(message => message.Properties.MessageId));
        var warning = Assert.Single(warnings);
        Assert.Contains("could not be made visible again", warning, StringComparison.Ordinal);
    }

    private sealed class StuckReleaseClient() : AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig { ServiceURL = "http://localhost" })
    {
        private bool _received;
        public override Task<ReceiveMessageResponse> ReceiveMessageAsync(ReceiveMessageRequest request, CancellationToken cancellationToken = default)
        {
            if (_received) return Task.FromResult(new ReceiveMessageResponse { Messages = [] });
            _received = true;
            return Task.FromResult(new ReceiveMessageResponse
            {
                Messages = new[] { "a", "b" }.Select(id => new Message
                {
                    MessageId = id, ReceiptHandle = id, Body = id, Attributes = new() { ["DeadLetterQueueSourceArn"] = QueueArn }
                }).ToList()
            });
        }
        public override Task<ChangeMessageVisibilityBatchResponse> ChangeMessageVisibilityBatchAsync(ChangeMessageVisibilityBatchRequest request, CancellationToken cancellationToken = default) =>
            Task.FromResult(new ChangeMessageVisibilityBatchResponse
            {
                Failed = [new BatchResultErrorEntry { Id = request.Entries[0].Id, Code = "InternalError", SenderFault = false, Message = "busy" }]
            });
    }

    private static AwsSqsSnsWorkspace CreateWorkspace(string root, MixedSqsClient broker) => CreateWorkspace(root, broker, broker.Ids.Length);

    private static AwsSqsSnsWorkspace CreateWorkspace(string root, AmazonSQSClient broker, int sharedCount)
    {
        var owner = new AwsSqsSnsWorkspace(new DeepAuditCloudTests.EmptyVault(), backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(root)));
        var shared = new AwsQueueInfo("shared", "http://localhost/shared", SharedArn, false, sharedCount, 0, 0, null, null, null);
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

    private sealed class EndlessOtherSourceClient() : AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig { ServiceURL = "http://localhost" })
    {
        private int _next;
        public List<string> Released { get; } = [];
        public override Task<ReceiveMessageResponse> ReceiveMessageAsync(ReceiveMessageRequest request, CancellationToken cancellationToken = default) =>
            Task.FromResult(new ReceiveMessageResponse
            {
                Messages = Enumerable.Range(0, request.MaxNumberOfMessages ?? 10).Select(_ => $"other-{_next++}").Select(id => new Message
                {
                    MessageId = id, ReceiptHandle = id, Body = id,
                    Attributes = new() { ["DeadLetterQueueSourceArn"] = "arn:aws:sqs:us-east-1:123:other" }
                }).ToList()
            });
        public override Task<ChangeMessageVisibilityBatchResponse> ChangeMessageVisibilityBatchAsync(ChangeMessageVisibilityBatchRequest request, CancellationToken cancellationToken = default)
        { Released.AddRange(request.Entries.Select(e => e.ReceiptHandle)); return Task.FromResult(new ChangeMessageVisibilityBatchResponse { Failed = [] }); }
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
