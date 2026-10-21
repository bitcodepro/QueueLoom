using System.Reflection;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Messaging;

namespace QueueLoom.Tests;

// SQS and Pub/Sub hold a received message for 180 s, set once when it is received. A search or "load all" over a large
// dead-letter queue can hold up to 20,000 messages and run far longer: the first ones came back mid-scan, were received
// again, used up the scan's limits, and on a Pub/Sub subscription with a dead-letter policy every repeated pull counted
// as another delivery attempt. Long loops now renew everything they hold at half the hold duration.
public sealed class HoldRenewalTests
{
    private const BindingFlags Any = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    [Fact]
    public async Task ALongScanRenewsEverythingItHoldsBeforeTheHoldEnds()
    {
        var time = new ManualTime();
        var channel = new SlowChannel(time, TimeSpan.FromSeconds(180), receives: 10, perReceive: TimeSpan.FromSeconds(30));
        var held = new List<LeasedMessage>();

        var messages = await LeasedMessagingWorkspace.ReceiveUpToAsync(channel, 1_000, held, CancellationToken.None, time);

        Assert.Equal(10, messages.Count);
        // 30 s per receive: renewed after 3 receives (90 s = half the hold), and again 90 s later.
        Assert.Equal([3, 6, 9], channel.Renewals.Select(renewal => renewal.Count));
        // No message had been held for as long as the hold when it was renewed.
        Assert.All(channel.Renewals, renewal => Assert.True(renewal.Max() < TimeSpan.FromSeconds(180)));
    }

    [Fact]
    public async Task AChannelWhoseHoldsDoNotExpireIsNeverRenewed()
    {
        var time = new ManualTime();
        var channel = new SlowChannel(time, hold: null, receives: 10, perReceive: TimeSpan.FromSeconds(30));

        await LeasedMessagingWorkspace.ReceiveUpToAsync(channel, 1_000, [], CancellationToken.None, time);

        Assert.Empty(channel.Renewals);
    }

    [Fact]
    public async Task SqsRenewsTheVisibilityOfHeldMessagesInBatchesAndToleratesFailures()
    {
        var client = new RenewingClient();
        var owner = new AwsSqsSnsWorkspace(new DeepAuditCloudTests.EmptyVault());
        typeof(AwsSqsSnsWorkspace).GetField("_sqs", Any)!.SetValue(owner, client);
        var queue = new AwsQueueInfo("q", "http://localhost/q", "arn:aws:sqs:us-east-1:123:q", false, 0, 0, 0, null, null, null);
        var type = typeof(AwsSqsSnsWorkspace).GetNestedType("SqsChannel", BindingFlags.NonPublic)!;
        var channel = (ILeasedMessageChannel)Activator.CreateInstance(type, Any, null,
            [owner, ServiceBusEntityReference.Queue("q"), ServiceBusSubQueue.DeadLetter, queue, null, false], null)!;

        await channel.RenewAsync(Enumerable.Range(0, 12).Select(index => Leased($"m-{index}")).ToArray(), CancellationToken.None);

        Assert.Equal(TimeSpan.FromSeconds(AwsSqsSnsWorkspace.HoldSeconds), channel.HoldDuration);
        Assert.Equal([10, 2], client.Batches.Select(batch => batch.Count));
        Assert.All(client.Batches.SelectMany(batch => batch), entry => Assert.Equal(AwsSqsSnsWorkspace.HoldSeconds, entry.VisibilityTimeout));
    }

    private static LeasedMessage Leased(string id) => new(new BrowsedMessage(ServiceBusEntityReference.Queue("q"), ServiceBusSubQueue.DeadLetter,
        LeasedMessageIdentity.SequenceNumberFor(id), "x"u8.ToArray(), new EditableMessageProperties(MessageId: id)), id);

    /// <summary>Delivers one new message per receive, each receive taking <paramref name="perReceive"/>.</summary>
    private sealed class SlowChannel(ManualTime time, TimeSpan? hold, int receives, TimeSpan perReceive) : ILeasedMessageChannel
    {
        private readonly Dictionary<string, DateTimeOffset> _heldSince = [];
        private int _received;
        /// <summary>For each renewal, how long each renewed message had been held at that moment.</summary>
        public List<List<TimeSpan>> Renewals { get; } = [];
        public string PhysicalName => "q";
        public int MaximumBatchSize => 10;
        public TimeSpan? HoldDuration => hold;

        public Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int maxMessages, CancellationToken cancellationToken)
        {
            time.Advance(perReceive);
            if (_received == receives) return Task.FromResult<IReadOnlyList<LeasedMessage>>([]);
            var message = Leased($"m-{_received++}");
            _heldSince[message.LeaseHandle] = time.Now;
            return Task.FromResult<IReadOnlyList<LeasedMessage>>([message]);
        }

        public Task RenewAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken)
        {
            Renewals.Add(messages.Select(message => time.Now - _heldSince[message.LeaseHandle]).ToList());
            foreach (var message in messages) _heldSince[message.LeaseHandle] = time.Now;
            return Task.CompletedTask;
        }

        public Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyCollection<LeasedMessage>>([]);
    }

    private sealed class RenewingClient()
        : AmazonSQSClient(new BasicAWSCredentials("test", "test"), new AmazonSQSConfig { ServiceURL = "http://localhost" })
    {
        public List<List<ChangeMessageVisibilityBatchRequestEntry>> Batches { get; } = [];

        public override Task<ChangeMessageVisibilityBatchResponse> ChangeMessageVisibilityBatchAsync(
            ChangeMessageVisibilityBatchRequest request, CancellationToken cancellationToken = default)
        {
            Batches.Add([.. request.Entries]);
            // The first entry's handle is no longer valid; that does not stop the renewal of the others.
            return Task.FromResult(new ChangeMessageVisibilityBatchResponse
            {
                Failed = [new BatchResultErrorEntry { Id = "0", Code = "ReceiptHandleIsInvalid", SenderFault = true, Message = "expired" }]
            });
        }
    }

    private sealed class ManualTime : TimeProvider
    {
        private long _ticks;
        public DateTimeOffset Now => DateTimeOffset.UnixEpoch.AddTicks(_ticks);
        public override DateTimeOffset GetUtcNow() => Now;
        public override long GetTimestamp() => _ticks;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public void Advance(TimeSpan by) => _ticks += by.Ticks;
    }
}
