using System.Reflection;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;
using static QueueLoom.Tests.ViewModelStateTests;

namespace QueueLoom.Tests;

public sealed class DeepAuditLeaseTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SelectiveDeleteReleasesTheWholeBatchOnSettlementFailureOrBackupCancellation(bool cancel)
    {
        using var directory = new TemporaryDirectory();
        var source = ServiceBusEntityReference.Queue("q");
        var selected = Lease(1); var unrelated = Lease(2);
        var channel = new FakeChannel([selected, unrelated]);
        using var cancellation = new CancellationTokenSource();
        if (cancel) channel.OnReceive = () => cancellation.Cancel();
        else channel.ThrowOnSettle = true;
        var store = new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(directory.Path));
        var backup = await store.CreateSessionAsync(CreateProfile("Test", EnvironmentKind.Test), DateTimeOffset.UtcNow, default);
        var key = new DeadLetterMessageKey(source, ServiceBusSubQueue.DeadLetter, 1, "m-1");
        var task = (Task<IReadOnlyList<DeadLetterMessageDeletionResult>>)typeof(LeasedMessagingWorkspace)
            .GetMethod("DeleteFromChannelAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(new QueueLoom.Infrastructure.Aws.AwsSqsSnsWorkspace(new DeepAuditCloudTests.EmptyVault()), [source, ServiceBusSubQueue.DeadLetter, new[] { key }, channel, 100, backup, 1, 1, null, cancellation.Token])!;
        var result = Assert.Single(await task);
        Assert.Equal(cancel ? DeadLetterMessageDeletionOutcome.Cancelled : DeadLetterMessageDeletionOutcome.Failed, result.Outcome);
        Assert.Equal(["lease-1", "lease-2"], channel.Released.Select(m => m.LeaseHandle).Order());
        Assert.Empty(channel.Settled);

        LeasedMessage Lease(int id) => new(new BrowsedMessage(source, ServiceBusSubQueue.DeadLetter, id, new byte[] { 1 },
            new EditableMessageProperties(MessageId: $"m-{id}")), $"lease-{id}");
    }

    internal sealed class FakeChannel(IReadOnlyList<LeasedMessage> batch) : ILeasedMessageChannel
    {
        private bool _received;
        public string PhysicalName => "isolated-test";
        public int MaximumBatchSize => 10;
        public bool ThrowOnSettle { get; set; }
        public Action? OnReceive { get; set; }
        public List<LeasedMessage> Released { get; } = [];
        public List<LeasedMessage> Settled { get; } = [];
        public Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int maxMessages, CancellationToken token)
        {
            if (_received) return Task.FromResult<IReadOnlyList<LeasedMessage>>([]);
            _received = true; OnReceive?.Invoke(); return Task.FromResult(batch);
        }
        public Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken token)
        { Released.AddRange(messages); return Task.CompletedTask; }
        public Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken token)
        {
            if (ThrowOnSettle) throw new IOException("isolated settlement failure");
            Settled.AddRange(messages); return Task.FromResult<IReadOnlyCollection<LeasedMessage>>([]);
        }
    }
}
