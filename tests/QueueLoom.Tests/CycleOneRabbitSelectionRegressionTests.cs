using System.Text;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.RabbitMq;
using QueueLoom.Tests.Infrastructure;
using RabbitMQ.Client;

namespace QueueLoom.Tests;

public sealed class CycleOneRabbitSelectionRegressionTests
{
    private static readonly ServiceBusEntityReference Source = ServiceBusEntityReference.Queue("orders");
    private static BrowsedMessage Message(string body) => RabbitMqMessageMapper.FromAmqp(Encoding.UTF8.GetBytes(body),
        new BasicProperties { MessageId = "repeated-id" }, "failed", Source, ServiceBusSubQueue.DeadLetter);
    private static ServiceBusProfile Profile => ServiceBusProfile.CreateNew("Isolated Rabbit", EnvironmentKind.Development,
        new(AuthenticationKind.RabbitMqPassword), accessMode: ProfileAccessMode.ReadWrite) with { Provider = MessagingProvider.RabbitMq };

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SelectedDeleteCannotAcknowledgeAnUnreviewedDeliveryWithTheSameApplicationId(bool selectBoth)
    {
        using var directory = new TemporaryDirectory();
        await using var workspace = new Workspace(directory.Path);
        await workspace.ConnectAsync(Profile);
        var selection = selectBoth ? workspace.Messages : [workspace.Messages[1]];
        var request = new DeleteDeadLetterMessagesRequest(selection.Select(m => new DeadLetterMessageKey(Source, m.SubQueue, m.SequenceNumber, m.Properties.MessageId)));
        DeleteDeadLetterMessagesResult? result = null;
        var error = await Record.ExceptionAsync(async () => result = await workspace.DeleteDeadLetterMessagesAsync(request));
        Assert.True(error is NotSupportedException, $"Selected deletion must fail closed: acknowledged [{string.Join(", ", workspace.Acknowledged)}], reported {result?.DeletedCount} selected message(s) deleted.");
        Assert.Empty(workspace.Acknowledged);
        Assert.Equal(2, workspace.Messages.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ImmediateAndDurableMovesAreBlockedBeforeSending(bool durable)
    {
        using var directory = new TemporaryDirectory();
        await using var workspace = new Workspace(directory.Path);
        var profile = Profile;
        await workspace.ConnectAsync(profile);
        var selected = workspace.Messages[1];
        var items = new[] { new ResendItem(selected, Source, selected.CreateDraft()) };
        if (durable)
        {
            var store = new BatchReplayStore(Path.Combine(directory.Path, "operations"));
            var plan = await store.CreateResendAsync(profile.Id, items, ResendMode.Move, 50, profile.EndpointDisplay,
                ScheduledResend.IdentityFor(profile), "Resend", default);
            var error = await Record.ExceptionAsync(() => store.RunItemsAsync(plan, [0], false, workspace, () => true, null, default));
            Assert.NotNull(error);
            Assert.Equal("Pending", Assert.Single(store.ReadHistory(plan).Items).State);
        }
        else Assert.NotNull(await Record.ExceptionAsync(() => DeadLetterResender.ResendAsync(workspace, items, ResendMode.Move)));
        Assert.Empty(workspace.Sent);
        Assert.Empty(workspace.Acknowledged);
    }

    [Fact]
    public async Task CopyAndBackedUpPurgeStillProcessBothDistinctDeliveries()
    {
        using var directory = new TemporaryDirectory();
        await using var workspace = new Workspace(directory.Path);
        await workspace.ConnectAsync(Profile);
        var copied = await DeadLetterResender.ResendAsync(workspace, workspace.Messages.Select(m => new ResendItem(m, Source, m.CreateDraft())).ToArray(), ResendMode.Copy);
        Assert.Equal(2, copied.SentCount);
        Assert.Equal(["A", "B"], workspace.Sent);
        Assert.Empty(workspace.Acknowledged);
        var purged = await workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest([new DeadLetterPurgeTarget(Source, ServiceBusSubQueue.DeadLetter)], batchSize: 1));
        Assert.Equal(2, purged.DeletedCount);
        Assert.Equal(["A", "B"], workspace.Acknowledged);
        Assert.Equal(2, (await new JsonDeadLetterBackupRepository(QueueLoomPaths.ForRoot(directory.Path)).ListAsync()).Count);
    }

    private sealed class Workspace(string root) : LeasedMessagingWorkspace(new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(root)), null)
    {
        public override MessagingProvider Provider => MessagingProvider.RabbitMq;
        public List<BrowsedMessage> Messages { get; } = [Message("A"), Message("B")];
        public List<string> Acknowledged { get; } = [];
        public List<string> Sent { get; } = [];
        protected override Task OpenAsync(ServiceBusProfile profile, CancellationToken token) => Task.CompletedTask;
        protected override ValueTask CloseAsync() => ValueTask.CompletedTask;
        protected override Task<ServiceBusTopology> ReadTopologyAsync(CancellationToken token) => Task.FromResult(new ServiceBusTopology(DateTimeOffset.UtcNow,
            [new ServiceBusQueue("orders", ServiceBusEntityRuntime.Empty)]));
        protected override ILeasedMessageChannel OpenChannel(ServiceBusTopology topology, ServiceBusEntityReference source, ServiceBusSubQueue subQueue) => new Channel(this);
        protected override Task SendCoreAsync(ServiceBusTopology topology, ServiceBusEntityReference target, MessageDraft message, CancellationToken token)
        { Sent.Add(Encoding.UTF8.GetString(message.Body.GetBytes())); return Task.CompletedTask; }
        private sealed class Channel(Workspace owner) : ILeasedMessageChannel
        {
            private readonly HashSet<BrowsedMessage> _held = [];
            public string PhysicalName => "parking";
            public int MaximumBatchSize => 1;
            public Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int maxMessages, CancellationToken token)
            {
                var messages = owner.Messages.Where(m => !_held.Contains(m)).Take(maxMessages).ToArray();
                foreach (var message in messages) _held.Add(message);
                return Task.FromResult<IReadOnlyList<LeasedMessage>>(messages.Select(m => new LeasedMessage(m, Encoding.UTF8.GetString(m.Body.Span)) { DeliveryIdentity = Encoding.UTF8.GetString(m.Body.Span) }).ToArray());
            }
            public Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken token) { _held.Clear(); return Task.CompletedTask; }
            public Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken token)
            {
                foreach (var message in messages) { owner.Acknowledged.Add(Encoding.UTF8.GetString(message.Message.Body.Span)); owner.Messages.Remove(message.Message); _held.Remove(message.Message); }
                return Task.FromResult<IReadOnlyCollection<LeasedMessage>>([]);
            }
        }
    }
}
