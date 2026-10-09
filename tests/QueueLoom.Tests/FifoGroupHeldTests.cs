using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// SQS FIFO hands out no more messages of a group while earlier messages of it are in flight (the dead-letter search
// already accounts for this). Deleting selected messages and purging held the messages they skipped, so the rest of the
// group never arrived: two empty receives then read as "the queue is exhausted". A selected message behind them was
// reported as not found, and a purge of one source in a shared queue reported success with that source's messages left.
public sealed class FifoGroupHeldTests
{
    private static readonly ServiceBusEntityReference Orders = ServiceBusEntityReference.Queue("orders");

    [Fact]
    public async Task DeletingAMessageBehindHeldMessagesOfItsGroupSaysItWasNotReached()
    {
        using var directory = new TemporaryDirectory();
        // One group: the first batch is messages 1-10, and SQS hands out 11 and 12 only after those are gone.
        await using var workspace = new FifoWorkspace(QueueLoomPaths.ForRoot(directory.Path), Enumerable.Range(1, 12).Select(Message).ToList());
        await workspace.ConnectAsync(Profile());

        var result = await workspace.DeleteDeadLetterMessagesAsync(new DeleteDeadLetterMessagesRequest([Key(12)]));

        var item = Assert.Single(result.Messages);
        Assert.NotEqual(DeadLetterMessageDeletionOutcome.Deleted, item.Outcome);
        Assert.Contains("FIFO", item.Detail, StringComparison.Ordinal);
        Assert.Contains("may still be", item.Detail, StringComparison.Ordinal);
        Assert.Empty(workspace.Settled);
    }

    [Fact]
    public async Task PurgingOneSourceBehindAnotherSourcesHeldMessagesIsNotReportedComplete()
    {
        using var directory = new TemporaryDirectory();
        // A shared FIFO dead-letter queue: the group starts with ten messages of another source, then two of "orders".
        var messages = Enumerable.Range(1, 10).Select(id => (Message(id), false))
            .Concat([(Message(11), true), (Message(12), true)]).ToList();
        await using var workspace = new FifoWorkspace(QueueLoomPaths.ForRoot(directory.Path), messages.Select(m => m.Item1).ToList(),
            belongs: message => messages.Single(m => m.Item1 == message).Item2);
        await workspace.ConnectAsync(Profile());

        var result = await workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest([Orders], [ServiceBusSubQueue.DeadLetter]));

        var source = Assert.Single(result.Sources);
        Assert.Equal(0, source.DeletedCount);
        Assert.False(source.IsSuccessful);
        Assert.Contains("FIFO", source.Error, StringComparison.Ordinal);
    }

    // Without a FIFO group in the way (all of the group is this source's), the purge still completes as before.
    [Fact]
    public async Task PurgingAGroupOfOnlyThisSourceStillCompletes()
    {
        using var directory = new TemporaryDirectory();
        await using var workspace = new FifoWorkspace(QueueLoomPaths.ForRoot(directory.Path), Enumerable.Range(1, 12).Select(Message).ToList());
        await workspace.ConnectAsync(Profile());

        var result = await workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest([Orders], [ServiceBusSubQueue.DeadLetter]));

        var source = Assert.Single(result.Sources);
        Assert.Equal(12, source.DeletedCount);
        Assert.True(source.IsSuccessful);
    }

    private static BrowsedMessage Message(int id) =>
        new(Orders, ServiceBusSubQueue.DeadLetter, id, new byte[] { 1 }, new EditableMessageProperties(MessageId: $"m-{id}", SessionId: "G"));

    private static DeadLetterMessageKey Key(int id) => new(Orders, ServiceBusSubQueue.DeadLetter, id, $"m-{id}");

    private static ServiceBusProfile Profile() =>
        ViewModelStateTests.CreateProfile("Fifo", EnvironmentKind.Development, ProfileAccessMode.ReadWrite) with { Provider = MessagingProvider.AmazonSqsSns };

    /// <summary>One FIFO group: a receive hands out up to ten of its first visible messages, none while any is held.</summary>
    private sealed class FifoWorkspace(QueueLoomPaths paths, List<BrowsedMessage> messages, Func<BrowsedMessage, bool>? belongs = null)
        : LeasedMessagingWorkspace(new DeadLetterJsonBackupStore(paths), null)
    {
        private readonly HashSet<BrowsedMessage> _held = [];
        private List<BrowsedMessage> Messages { get; } = messages;
        private Func<BrowsedMessage, bool>? Belongs { get; } = belongs;
        public List<BrowsedMessage> Settled { get; } = [];
        public override MessagingProvider Provider => MessagingProvider.AmazonSqsSns;
        protected override Task OpenAsync(ServiceBusProfile profile, CancellationToken token) => Task.CompletedTask;
        protected override ValueTask CloseAsync() => ValueTask.CompletedTask;
        protected override Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken token) =>
            Task.FromResult(new ServiceBusTopology(DateTimeOffset.UtcNow, [new ServiceBusQueue("orders", ServiceBusEntityRuntime.Empty)]));
        protected override ILeasedMessageChannel OpenChannel(ServiceBusTopology topology, ServiceBusEntityReference source, ServiceBusSubQueue subQueue) =>
            new Channel(this);
        protected override Task SendCoreAsync(ServiceBusTopology topology, ServiceBusEntityReference target, MessageDraft draft, CancellationToken token) =>
            Task.CompletedTask;

        private sealed class Channel(FifoWorkspace owner) : ILeasedMessageChannel
        {
            public string PhysicalName => "orders-dlq.fifo";
            public int MaximumBatchSize => 10;
            public bool ReadsOneBatchPerMessageGroup => true;

            public Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int count, CancellationToken token)
            {
                IReadOnlyList<LeasedMessage> batch = owner._held.Count > 0
                    ? []
                    : owner.Messages.Take(Math.Min(count, 10)).Select(message =>
                        new LeasedMessage(message, "lease-" + message.SequenceNumber, owner.Belongs?.Invoke(message) ?? true)).ToArray();
                foreach (var leased in batch) owner._held.Add(leased.Message);
                return Task.FromResult(batch);
            }

            public Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> leased, CancellationToken token)
            {
                foreach (var message in leased) owner._held.Remove(message.Message);
                return Task.CompletedTask;
            }

            public Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(IReadOnlyCollection<LeasedMessage> leased, CancellationToken token)
            {
                foreach (var message in leased)
                {
                    owner._held.Remove(message.Message);
                    owner.Messages.Remove(message.Message);
                    owner.Settled.Add(message.Message);
                }
                return Task.FromResult<IReadOnlyCollection<LeasedMessage>>([]);
            }
        }
    }
}
