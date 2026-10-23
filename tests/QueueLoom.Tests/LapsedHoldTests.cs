using System.Reflection;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;
using static QueueLoom.Tests.ViewModelStateTests;

namespace QueueLoom.Tests;

// SQS and Pub/Sub hold a received message only for a while (180 s), renewed best effort. Once a hold has ended the broker
// may hand the message to another consumer, and on a Pub/Sub subscription without exactly-once delivery acknowledging
// the expired ack ID still succeeds. A purge or delete settled such a message and counted it as deleted although it was
// not, while its backup made a later restore a duplicate. A message whose hold may have ended is no longer settled.
public sealed class LapsedHoldTests
{
    private static readonly TimeSpan Hold = TimeSpan.FromSeconds(180);
    private static readonly ServiceBusEntityReference Source = ServiceBusEntityReference.Queue("q");

    [Fact]
    public async Task OnlyARenewalThatSucceededExtendsAHold()
    {
        var time = new ManualTime();
        var renewedOk = Leased(1);
        var renewalFailed = Leased(2);
        var channel = new Channel(time, [], receiveTakes: TimeSpan.Zero) { FailRenewalOf = [renewalFailed] };
        var renewal = new HoldRenewal(channel, time);
        renewal.Received([renewedOk, renewalFailed], renewal.Now());

        time.Advance(TimeSpan.FromSeconds(60));
        await renewal.RenewIfDueAsync([renewedOk, renewalFailed], CancellationToken.None);
        time.Advance(TimeSpan.FromSeconds(110));

        Assert.False(renewal.HasLapsed(renewedOk));
        Assert.True(renewal.HasLapsed(renewalFailed));
    }

    [Theory]
    [InlineData(170, false)]
    [InlineData(5, true)]
    public async Task APurgeDoesNotCountAMessageWhoseHoldMayHaveEnded(int receiveSeconds, bool deleted)
    {
        using var directory = new TemporaryDirectory();
        var time = new ManualTime();
        var channel = new Channel(time, [Leased(1)], TimeSpan.FromSeconds(receiveSeconds));
        var backup = await CreateBackupAsync(directory);

        var result = await (Task<DeadLetterPurgeSourceResult>)typeof(LeasedMessagingWorkspace)
            .GetMethod("PurgeTargetAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(new AwsSqsSnsWorkspace(new DeepAuditCloudTests.EmptyVault(), time),
                [new DeadLetterPurgeTarget(Source, ServiceBusSubQueue.DeadLetter), channel, 10, 100, backup, 1, 1, null, CancellationToken.None])!;

        if (deleted)
        {
            Assert.Equal(1, result.DeletedCount);
            Assert.Single(channel.Settled);
            return;
        }
        Assert.Equal(0, result.DeletedCount);
        Assert.Empty(channel.Settled);
        Assert.Contains("were not deleted: they may have been delivered again", result.Error);
    }

    [Theory]
    [InlineData(170, false)]
    [InlineData(5, true)]
    public async Task ASelectedDeleteDoesNotReportAMessageWhoseHoldMayHaveEndedAsDeleted(int receiveSeconds, bool deleted)
    {
        using var directory = new TemporaryDirectory();
        var time = new ManualTime();
        var channel = new Channel(time, [Leased(1)], TimeSpan.FromSeconds(receiveSeconds));
        var backup = await CreateBackupAsync(directory);
        var key = new DeadLetterMessageKey(Source, ServiceBusSubQueue.DeadLetter, 1, "m-1");

        var result = Assert.Single(await (Task<IReadOnlyList<DeadLetterMessageDeletionResult>>)typeof(LeasedMessagingWorkspace)
            .GetMethod("DeleteFromChannelAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(new AwsSqsSnsWorkspace(new DeepAuditCloudTests.EmptyVault(), time),
                [Source, ServiceBusSubQueue.DeadLetter, new[] { key }, channel, 100, backup, 1, 1, null, CancellationToken.None, null])!);

        if (deleted)
        {
            Assert.Equal(DeadLetterMessageDeletionOutcome.Deleted, result.Outcome);
            return;
        }
        Assert.Equal(DeadLetterMessageDeletionOutcome.Failed, result.Outcome);
        Assert.Contains("it may have been delivered again", result.Detail);
        Assert.Empty(channel.Settled);
    }

    private static async Task<DeadLetterJsonBackupSession> CreateBackupAsync(TemporaryDirectory directory) =>
        await new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(directory.Path))
            .CreateSessionAsync(CreateProfile("Test", EnvironmentKind.Test), DateTimeOffset.UtcNow, default);

    private static LeasedMessage Leased(int id) => new(new BrowsedMessage(Source, ServiceBusSubQueue.DeadLetter, id, new byte[] { 1 },
        new EditableMessageProperties(MessageId: $"m-{id}")), $"lease-{id}");

    /// <summary>Hands out its batch once; the receive takes <paramref name="receiveTakes"/>, and the hold starts during it.</summary>
    private sealed class Channel(ManualTime time, IReadOnlyList<LeasedMessage> batch, TimeSpan receiveTakes) : ILeasedMessageChannel
    {
        private bool _received;
        public string PhysicalName => "q";
        public int MaximumBatchSize => 10;
        public TimeSpan? HoldDuration => Hold;
        public IReadOnlyCollection<LeasedMessage> FailRenewalOf { get; init; } = [];
        public List<LeasedMessage> Settled { get; } = [];

        public Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int maxMessages, CancellationToken cancellationToken)
        {
            time.Advance(receiveTakes);
            if (_received) return Task.FromResult<IReadOnlyList<LeasedMessage>>([]);
            _received = true;
            return Task.FromResult(batch);
        }

        public Task<IReadOnlyCollection<LeasedMessage>> RenewAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyCollection<LeasedMessage>>(messages.Where(FailRenewalOf.Contains).ToList());

        public Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken)
        {
            Settled.AddRange(messages);
            return Task.FromResult<IReadOnlyCollection<LeasedMessage>>([]);
        }
    }

    private sealed class ManualTime : TimeProvider
    {
        private long _ticks;
        public override DateTimeOffset GetUtcNow() => DateTimeOffset.UnixEpoch.AddTicks(_ticks);
        public override long GetTimestamp() => _ticks;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public void Advance(TimeSpan by) => _ticks += by.Ticks;
    }
}
