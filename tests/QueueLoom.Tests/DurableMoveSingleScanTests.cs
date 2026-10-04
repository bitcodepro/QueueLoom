using System.Text;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class DurableMoveSingleScanTests
{
    private static readonly ServiceBusEntityReference Source = ServiceBusEntityReference.Queue("orders");

    [Fact]
    public async Task DurableMoveRemovesAllOriginalsInOneBackedUpScan()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        await using var workspace = new SqsLikeWorkspace(paths);
        var profile = ViewModelStateTests.CreateProfile("Sqs", EnvironmentKind.Development, ProfileAccessMode.ReadWrite)
            with { Provider = MessagingProvider.AmazonSqsSns };
        await workspace.ConnectAsync(profile);
        var store = new BatchReplayStore(Path.Combine(directory.Path, "operations")) { DelayAsync = (_, _) => Task.CompletedTask };
        // Three selected dead letters, two unrelated ones that another team still has to inspect.
        var selected = workspace.Messages.Where(m => m.Properties.MessageId!.StartsWith("sel-", StringComparison.Ordinal)).ToArray();
        var items = selected.Select(m => new ResendItem(m, ServiceBusEntityReference.Queue("orders-retry"), m.CreateDraft()).WithNewMessageId()).ToArray();
        var plan = await store.CreateResendAsync(profile.Id, items, ResendMode.Move, 50, profile.EndpointDisplay,
            ScheduledResend.IdentityFor(profile), "Immediate resend", default);

        var result = await store.RunItemsAsync(plan, [0, 1, 2], false, workspace, () => true, null, default);

        Assert.Equal(3, result.MovedCount);
        // Like DeadLetterResender, all sent originals go in ONE backed-up delete: each unrelated dead letter is
        // leased (SQS receive count / Pub/Sub delivery attempt / ASB DeliveryCount +1) once, not once per moved
        // item; one backup session is created and the reported folder holds every removed original.
        var sessions = Directory.GetDirectories(paths.BackupsDirectory, "*", SearchOption.AllDirectories)
            .Count(d => File.Exists(Path.Combine(d, "session.json")));
        var inReportedBackup = Directory.GetFiles(result.BackupDirectory!, "*.json", SearchOption.AllDirectories)
            .Count(f => Path.GetFileName(f) != "session.json");
        Assert.Equal("other-1 leased 1x, other-2 leased 1x, 1 backup session(s), 3 original(s) in reported backup",
            $"other-1 leased {workspace.Receives["other-1"]}x, other-2 leased {workspace.Receives["other-2"]}x, " +
            $"{sessions} backup session(s), {inReportedBackup} original(s) in reported backup");
    }

    private sealed class SqsLikeWorkspace(QueueLoomPaths paths) : LeasedMessagingWorkspace(new DeadLetterJsonBackupStore(paths), null)
    {
        public override MessagingProvider Provider => MessagingProvider.AmazonSqsSns;
        public List<BrowsedMessage> Messages { get; } =
            new[] { "sel-1", "other-1", "sel-2", "other-2", "sel-3" }.Select(id => new BrowsedMessage(Source, ServiceBusSubQueue.DeadLetter,
                LeasedMessageIdentity.SequenceNumberFor(id), Encoding.UTF8.GetBytes(id), new EditableMessageProperties(MessageId: id))).ToList();
        public Dictionary<string, int> Receives { get; } = new(StringComparer.Ordinal);
        protected override Task OpenAsync(ServiceBusProfile profile, CancellationToken token) => Task.CompletedTask;
        protected override ValueTask CloseAsync() => ValueTask.CompletedTask;
        protected override Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken token) => Task.FromResult(new ServiceBusTopology(DateTimeOffset.UtcNow,
            [new ServiceBusQueue("orders", ServiceBusEntityRuntime.Empty), new ServiceBusQueue("orders-retry", ServiceBusEntityRuntime.Empty)]));
        protected override ILeasedMessageChannel OpenChannel(ServiceBusTopology topology, ServiceBusEntityReference source, ServiceBusSubQueue subQueue) => new Channel(this);
        protected override Task SendCoreAsync(ServiceBusTopology topology, ServiceBusEntityReference target, MessageDraft draft, CancellationToken token) => Task.CompletedTask;

        private sealed class Channel(SqsLikeWorkspace owner) : ILeasedMessageChannel
        {
            private readonly HashSet<BrowsedMessage> _invisible = [];
            public string PhysicalName => "orders-dlq";
            public int MaximumBatchSize => 10;
            public Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int maxMessages, CancellationToken token)
            {
                var batch = owner.Messages.Where(m => !_invisible.Contains(m)).Take(maxMessages).ToArray();
                foreach (var message in batch)
                {
                    _invisible.Add(message);
                    var id = message.Properties.MessageId!;
                    owner.Receives[id] = owner.Receives.GetValueOrDefault(id) + 1; // ApproximateReceiveCount
                }
                return Task.FromResult<IReadOnlyList<LeasedMessage>>(batch.Select(m => new LeasedMessage(m, "rh-" + m.Properties.MessageId)).ToArray());
            }
            public Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken token)
            {
                foreach (var message in messages) _invisible.Remove(message.Message);
                return Task.CompletedTask;
            }
            public Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken token)
            {
                foreach (var message in messages) owner.Messages.Remove(message.Message);
                return Task.FromResult<IReadOnlyCollection<LeasedMessage>>([]);
            }
        }
    }
}
