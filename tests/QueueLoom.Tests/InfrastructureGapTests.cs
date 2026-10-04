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

/// <summary>Gap 2 against the SQS workspace, with a broker that blocks a FIFO group while some of it is held.</summary>
public sealed class SqsFifoGroupBlockingTests
{
    private const string QueueArn = "arn:aws:sqs:us-east-1:123:orders.fifo";
    private const string DeadLetterArn = "arn:aws:sqs:us-east-1:123:orders-dlq.fifo";

    [Fact]
    public async Task DeadLetterSearchOfAFifoQueueIsIncompleteWhileAGroupWasHeld()
    {
        using var directory = new TemporaryDirectory();
        var broker = new FifoGroupSqs(Enumerable.Range(1, 25).Select(index => ($"g-{index}", (string?)"g")).ToArray());
        await using var workspace = CreateWorkspace(directory.Path, broker, fifo: true);
        var source = ServiceBusEntityReference.Queue("orders.fifo");

        var browsed = await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(source, ServiceBusSubQueue.DeadLetter));
        var search = await workspace.SearchDeadLettersAsync(new DeadLetterSearchRequest("g-",
            [new DeadLetterSearchTarget(source, ServiceBusSubQueue.DeadLetter, 25)]));

        // The broker behaves as SQS does: one batch of the group, then nothing while it is held.
        Assert.Equal(10, browsed.Count);
        Assert.Equal(10, search.ScannedMessageCount);
        Assert.False(search.IsComplete);
        Assert.Equal(0, broker.InFlight);
    }

    [Fact]
    public async Task DeadLetterSearchOfAStandardQueueThatRanDryIsComplete()
    {
        using var directory = new TemporaryDirectory();
        var broker = new FifoGroupSqs(Enumerable.Range(1, 5).Select(index => ($"m-{index}", (string?)null)).ToArray());
        await using var workspace = CreateWorkspace(directory.Path, broker, fifo: false);
        var source = ServiceBusEntityReference.Queue("orders.fifo");

        var search = await workspace.SearchDeadLettersAsync(new DeadLetterSearchRequest("m-",
            [new DeadLetterSearchTarget(source, ServiceBusSubQueue.DeadLetter, 5)]));

        Assert.Equal(5, search.ScannedMessageCount);
        Assert.True(search.IsComplete);
    }

    private static AwsSqsSnsWorkspace CreateWorkspace(string root, AmazonSQSClient broker, bool fifo)
    {
        var owner = new AwsSqsSnsWorkspace(new DeepAuditCloudTests.EmptyVault(),
            backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(root)));
        var queue = new AwsQueueInfo("orders.fifo", "http://localhost/orders.fifo", QueueArn, fifo, 0, 0, 0, DeadLetterArn, null, null);
        var deadLetterQueue = new AwsQueueInfo("orders-dlq.fifo", "http://localhost/orders-dlq.fifo", DeadLetterArn, fifo, 25, 0, 0,
            null, null, null);
        var index = new AwsTopologyIndex([queue, deadLetterQueue], []);
        var profile = ViewModelStateTests.CreateProfile("Test", EnvironmentKind.Test) with { Provider = MessagingProvider.AmazonSqsSns };
        Set(typeof(AwsSqsSnsWorkspace), "_index", index);
        Set(typeof(AwsSqsSnsWorkspace), "_sqs", broker);
        Set(typeof(LeasedMessagingWorkspace), "_profile", profile);
        Set(typeof(LeasedMessagingWorkspace), "_connectionState", WorkspaceConnectionState.Connected);
        Set(typeof(LeasedMessagingWorkspace), "_cachedTopology", index.ToTopology(DateTimeOffset.UtcNow));
        return owner;
        void Set(Type type, string field, object value) =>
            type.GetField(field, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner, value);
    }

    /// <summary>SQS FIFO: no message of a group is handed out while another message of that group is in flight.</summary>
    private sealed class FifoGroupSqs((string Id, string? Group)[] messages)
        : AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig { ServiceURL = "http://localhost" })
    {
        private readonly HashSet<string> _inFlight = new(StringComparer.Ordinal);

        public int InFlight => _inFlight.Count;

        public override Task<ReceiveMessageResponse> ReceiveMessageAsync(ReceiveMessageRequest request,
            CancellationToken cancellationToken = default)
        {
            var blocked = messages.Where(message => message.Group is not null && _inFlight.Contains(message.Id))
                .Select(message => message.Group!).ToHashSet(StringComparer.Ordinal);
            var batch = messages
                .Where(message => !_inFlight.Contains(message.Id) && (message.Group is null || !blocked.Contains(message.Group)))
                .Take(request.MaxNumberOfMessages ?? 1)
                .ToArray();
            foreach (var message in batch)
            {
                _inFlight.Add(message.Id);
            }
            return Task.FromResult(new ReceiveMessageResponse
            {
                Messages = batch.Select(message => new Message
                {
                    MessageId = message.Id,
                    ReceiptHandle = message.Id,
                    Body = message.Id,
                    Attributes = message.Group is null
                        ? new() { ["DeadLetterQueueSourceArn"] = QueueArn }
                        : new() { ["DeadLetterQueueSourceArn"] = QueueArn, ["MessageGroupId"] = message.Group }
                }).ToList()
            });
        }

        public override Task<ChangeMessageVisibilityBatchResponse> ChangeMessageVisibilityBatchAsync(
            ChangeMessageVisibilityBatchRequest request, CancellationToken cancellationToken = default)
        {
            foreach (var entry in request.Entries)
            {
                _inFlight.Remove(entry.ReceiptHandle);
            }
            return Task.FromResult(new ChangeMessageVisibilityBatchResponse { Failed = [] });
        }
    }
}

/// <summary>Gap 3: closing a Pub/Sub connection dropped its gRPC clients without shutting their channels down.</summary>
public sealed class PubSubChannelShutdownTests
{
    private static readonly FieldInfo? ChannelsField =
        typeof(QueueLoom.Infrastructure.Google.GooglePubSubWorkspace).GetField("_channels", BindingFlags.Instance | BindingFlags.NonPublic);

    [Fact]
    public async Task ClosingTheConnectionShutsDownEveryChannelItCreated()
    {
        await using var workspace = new QueueLoom.Infrastructure.Google.GooglePubSubWorkspace(new DeepAuditCloudTests.EmptyVault());
        Assert.NotNull(ChannelsField);
        var channels = (List<Grpc.Core.ChannelBase>)ChannelsField.GetValue(workspace)!;
        var publisher = new RecordingChannel("publisher");
        var subscriber = new RecordingChannel("subscriber");
        channels.Add(publisher);
        channels.Add(subscriber);

        await workspace.DisconnectAsync();

        Assert.True(publisher.ShutDown);
        Assert.True(subscriber.ShutDown);
        Assert.Empty(channels);
    }

    [Fact]
    public async Task AFailedEmulatorConnectionLeavesNoOpenChannel()
    {
        await using var workspace = new QueueLoom.Infrastructure.Google.GooglePubSubWorkspace(new DeepAuditCloudTests.EmptyVault());
        Assert.NotNull(ChannelsField);
        var profile = ViewModelStateTests.CreateProfile("Emulator", EnvironmentKind.Development) with
        {
            Provider = MessagingProvider.GooglePubSub,
            GooglePubSub = new GooglePubSubSettings("project-a", "127.0.0.1:1")
        };
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var open = (Task)typeof(QueueLoom.Infrastructure.Google.GooglePubSubWorkspace)
            .GetMethod("OpenAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(workspace, [profile, timeout.Token])!;
        await Assert.ThrowsAnyAsync<Exception>(() => open);
        // The publisher and subscriber each got their own channel to the emulator.
        var created = ((List<Grpc.Core.ChannelBase>)ChannelsField.GetValue(workspace)!).ToArray();
        Assert.Equal(2, created.Length);

        await workspace.DisconnectAsync();

        Assert.Empty((List<Grpc.Core.ChannelBase>)ChannelsField.GetValue(workspace)!);
        Assert.All(created, channel =>
        {
            var disposed = channel.GetType().GetProperty("Disposed", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
            Assert.NotNull(disposed);
            Assert.True((bool)disposed.GetValue(channel)!);
        });
    }

    private sealed class RecordingChannel(string target) : Grpc.Core.ChannelBase(target)
    {
        public bool ShutDown { get; private set; }

        public override Grpc.Core.CallInvoker CreateCallInvoker() => throw new NotSupportedException();

        protected override Task ShutdownAsyncCore()
        {
            ShutDown = true;
            return Task.CompletedTask;
        }
    }
}

/// <summary>Gap 5: deleting the last message file of a backup session left a folder holding only session.json.</summary>
public sealed class BackupSessionCleanupTests
{
    [Fact]
    public async Task DeletingTheLastMessageOfAFinishedSessionRemovesTheSessionFolder()
    {
        using var directory = new TemporaryDirectory();
        var (repository, session) = await CreateSessionAsync(directory.Path, messages: 2, quietFor: TimeSpan.FromDays(3));
        var backups = await repository.ListAsync();
        Assert.Equal(2, backups.Count);

        await repository.DeleteAsync(backups[0]);
        // A session that still has a message stays, session.json included.
        Assert.True(File.Exists(Path.Combine(session, "session.json")));

        await repository.DeleteAsync(backups[1]);

        Assert.False(Directory.Exists(session));
        // The date folder held only that session, so it goes as well.
        Assert.False(Directory.Exists(Path.GetDirectoryName(session)));
        Assert.Empty(await repository.ListAsync());
    }

    [Fact]
    public async Task ASessionThatMayStillBeWrittenKeepsItsFolder()
    {
        using var directory = new TemporaryDirectory();
        var (repository, session) = await CreateSessionAsync(directory.Path, messages: 1, quietFor: TimeSpan.Zero);

        await repository.DeleteAsync(Assert.Single(await repository.ListAsync()));

        Assert.True(File.Exists(Path.Combine(session, "session.json")));
    }

    private static async Task<(JsonDeadLetterBackupRepository Repository, string Session)> CreateSessionAsync(
        string root, int messages, TimeSpan quietFor)
    {
        var paths = QueueLoomPaths.ForRoot(root);
        var profile = ViewModelStateTests.CreateProfile("Test", EnvironmentKind.Test);
        var session = await new DeadLetterJsonBackupStore(paths)
            .CreateSessionAsync(profile, DateTimeOffset.UtcNow.AddDays(-3), CancellationToken.None);
        for (var index = 1; index <= messages; index++)
        {
            await session.BackupAsync(ViewModelStateTests.SearchMessage(ServiceBusEntityReference.Queue("orders"), index,
                "2026-09-01T10:00:00Z"), CancellationToken.None);
        }
        File.SetLastWriteTimeUtc(Path.Combine(session.RootDirectory, "session.json"), DateTime.UtcNow - quietFor);
        return (new JsonDeadLetterBackupRepository(paths), session.RootDirectory);
    }
}
