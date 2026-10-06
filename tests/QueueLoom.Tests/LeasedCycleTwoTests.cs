using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class LeasedCycleTwoTests
{
    private static readonly ServiceBusEntityReference Source = ServiceBusEntityReference.Queue("orders");

    [Theory]
    [InlineData(MessagingProvider.AmazonSqsSns)]
    [InlineData(MessagingProvider.GooglePubSub)]
    public async Task BugCycleTwo_CancellationDuringAcceptedSettlementRemainsUncertain(MessagingProvider provider)
    {
        using var directory = new TemporaryDirectory();
        using var cancellation = new CancellationTokenSource();
        await using var workspace = new TestWorkspace(QueueLoomPaths.ForRoot(directory.Path), provider) { CancelAfterAcceptingSecond = cancellation };
        await workspace.ConnectAsync(Profile(provider));
        var selected = workspace.Messages.Take(3).ToArray();
        var result = await workspace.DeleteDeadLetterMessagesAsync(new DeleteDeadLetterMessagesRequest(selected.Select(Key)), cancellation.Token);
        Assert.Equal(DeadLetterMessageDeletionOutcome.Deleted, result.Messages[0].Outcome);
        Assert.Equal(DeadLetterMessageDeletionOutcome.Failed, result.Messages[1].Outcome);
        Assert.Contains("unknown", result.Messages[1].Detail, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(DeadLetterMessageDeletionOutcome.Cancelled, result.Messages[2].Outcome);
        Assert.Equal(2, workspace.Settled.Count);
        Assert.DoesNotContain(selected[0], workspace.Released);
        Assert.Contains(selected[2], workspace.Released);
    }

    [Theory]
    [InlineData(MessagingProvider.AmazonSqsSns, false)]
    [InlineData(MessagingProvider.GooglePubSub, false)]
    [InlineData(MessagingProvider.AmazonSqsSns, true)]
    [InlineData(MessagingProvider.GooglePubSub, true)]
    public async Task BugCycleTwo_ConfirmedDeleteAndMoveRetainUnrelatedReleaseWarning(MessagingProvider provider, bool resend)
    {
        using var directory = new TemporaryDirectory();
        await using var workspace = new TestWorkspace(QueueLoomPaths.ForRoot(directory.Path), provider) { FailRelease = true };
        await workspace.ConnectAsync(Profile(provider));
        var selected = workspace.Messages[0];
        IReadOnlyList<string> warnings;
        if (resend)
        {
            var item = new ResendItem(selected, ServiceBusEntityReference.Queue("retry"), selected.CreateDraft()).WithNewMessageId();
            var result = await DeadLetterResender.ResendAsync(workspace, [item], ResendMode.Move, 0, null, default);
            Assert.Equal(1, result.SentCount);
            Assert.Equal(1, result.MovedCount);
            warnings = result.Warnings;
        }
        else
        {
            var result = await workspace.DeleteDeadLetterMessagesAsync(new DeleteDeadLetterMessagesRequest([Key(selected)]));
            Assert.Equal(DeadLetterMessageDeletionOutcome.Deleted, Assert.Single(result.Messages).Outcome);
            warnings = result.Warnings;
        }
        Assert.Contains("isolated release failure", Assert.Single(warnings), StringComparison.Ordinal);
        Assert.Single(workspace.Settled);
        Assert.DoesNotContain(selected, workspace.Released);
        Assert.Equal(3, workspace.Released.Count);
    }

    private static ServiceBusProfile Profile(MessagingProvider provider) => ViewModelStateTests.CreateProfile("Cycle two", EnvironmentKind.Development, ProfileAccessMode.ReadWrite) with { Provider = provider };
    private static DeadLetterMessageKey Key(BrowsedMessage message) => new(Source, ServiceBusSubQueue.DeadLetter, message.SequenceNumber, message.Properties.MessageId);

    private sealed class TestWorkspace(QueueLoomPaths paths, MessagingProvider provider) : LeasedMessagingWorkspace(new DeadLetterJsonBackupStore(paths), null)
    {
        public override MessagingProvider Provider => provider;
        public List<BrowsedMessage> Messages { get; } = Enumerable.Range(1, 4).Select(id => new BrowsedMessage(Source, ServiceBusSubQueue.DeadLetter, id, new byte[] { 1 }, new EditableMessageProperties(MessageId: $"m-{id}"))).ToList();
        public List<BrowsedMessage> Settled { get; } = [];
        public List<BrowsedMessage> Released { get; } = [];
        public CancellationTokenSource? CancelAfterAcceptingSecond { get; init; }
        public bool FailRelease { get; init; }
        protected override Task OpenAsync(ServiceBusProfile profile, CancellationToken token) => Task.CompletedTask;
        protected override ValueTask CloseAsync() => ValueTask.CompletedTask;
        protected override Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken token) => Task.FromResult(new ServiceBusTopology(DateTimeOffset.UtcNow, [new ServiceBusQueue("orders", ServiceBusEntityRuntime.Empty), new ServiceBusQueue("retry", ServiceBusEntityRuntime.Empty)]));
        protected override ILeasedMessageChannel OpenChannel(ServiceBusTopology topology, ServiceBusEntityReference source, ServiceBusSubQueue subQueue) => new Channel(this);
        protected override Task SendCoreAsync(ServiceBusTopology topology, ServiceBusEntityReference target, MessageDraft draft, CancellationToken token) => Task.CompletedTask;

        private sealed class Channel(TestWorkspace owner) : ILeasedMessageChannel
        {
            private bool _received;
            public string PhysicalName => "orders-dlq";
            public int MaximumBatchSize => 10;
            public Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int count, CancellationToken token)
            {
                if (_received) return Task.FromResult<IReadOnlyList<LeasedMessage>>([]);
                _received = true;
                return Task.FromResult<IReadOnlyList<LeasedMessage>>(owner.Messages.Select(m => new LeasedMessage(m, "lease-" + m.SequenceNumber)).ToArray());
            }
            public Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken token)
            {
                owner.Released.AddRange(messages.Select(m => m.Message));
                if (owner.FailRelease) throw new IOException("isolated release failure");
                return Task.CompletedTask;
            }
            public Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken token)
            {
                Assert.False(token.CanBeCanceled);
                owner.Settled.AddRange(messages.Select(m => m.Message));
                foreach (var message in messages) owner.Messages.Remove(message.Message);
                if (owner.Settled.Count == 2 && owner.CancelAfterAcceptingSecond is { } cancellation)
                {
                    cancellation.Cancel();
                    throw new OperationCanceledException("settlement accepted but acknowledgement interrupted");
                }
                return Task.FromResult<IReadOnlyCollection<LeasedMessage>>([]);
            }
        }
    }
}
