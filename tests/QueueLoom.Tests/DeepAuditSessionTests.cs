using System.Reflection;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

public sealed class DeepAuditSessionTests
{
    [Fact]
    public async Task SessionLimitFailsExplicitlyBeforeReturningAnIncompleteGlobalPage()
    {
        var sessions = Enumerable.Range(1, 101).Reverse().Select(i => new[] { Message(i) }).ToArray();
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => Browse(ServiceBusEntityReference.Queue("orders"), sessions, null));
        Assert.Contains("global", error.Message, StringComparison.OrdinalIgnoreCase);
    }
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SessionBrowseHonorsTheSecondPageCursorForQueuesAndSubscriptions(bool subscription)
    {
        var source = subscription ? ServiceBusEntityReference.Subscription("events", "billing") : ServiceBusEntityReference.Queue("orders");
        var messages = Enumerable.Range(1, 101).Select(i => Message(i)).ToArray();
        var first = await Browse(source, [messages], null);
        var second = await Browse(source, [messages], 101);
        Assert.Equal(Enumerable.Range(1, 100).Select(i => (long)i), first.Select(m => m.SequenceNumber));
        Assert.Equal([101L], second.Select(m => m.SequenceNumber));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InterleavedSessionsUseOneGlobalOrderWithoutSkippingMessages(bool subscription)
    {
        var source = subscription ? ServiceBusEntityReference.Subscription("events", "billing") : ServiceBusEntityReference.Queue("orders");
        ServiceBusReceivedMessage[][] sessions =
        [Enumerable.Range(1, 100).Select(i => Message(i * 2 - 1)).ToArray(), Enumerable.Range(1, 100).Select(i => Message(i * 2)).ToArray()];
        var first = await Browse(source, sessions, null);
        var second = await Browse(source, sessions, first.Max(m => m.SequenceNumber) + 1);
        Assert.Equal(Enumerable.Range(1, 200).Select(i => (long)i), first.Concat(second).Select(m => m.SequenceNumber));
    }

    private static ServiceBusReceivedMessage Message(int sequence) => ServiceBusModelFactory.ServiceBusReceivedMessage(
        body: BinaryData.FromString("{}"), messageId: $"m-{sequence}", sequenceNumber: sequence);

    private static async Task<IReadOnlyList<BrowsedMessage>> Browse(ServiceBusEntityReference source,
        ServiceBusReceivedMessage[][] sessions, long? cursor)
    {
        await using var owner = new AzureServiceBusWorkspace(new DeepAuditCloudTests.EmptyVault());
        var client = new SessionClient(sessions);
        typeof(AzureServiceBusWorkspace).GetField("_client", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner, client);
        var task = (Task<IReadOnlyList<BrowsedMessage>>)typeof(AzureServiceBusWorkspace)
            .GetMethod("BrowseSessionsAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(owner, [new BrowseMessagesRequest(source, maxMessages: 100, fromSequenceNumber: cursor), CancellationToken.None])!;
        try { return await task; }
        finally { Assert.All(client.Receivers, receiver => Assert.True(receiver.Disposed)); }
    }

    private sealed class SessionClient(ServiceBusReceivedMessage[][] sessions) : ServiceBusClient
    {
        private int _next;
        public List<SessionReceiver> Receivers { get; } = [];
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
        private Task<ServiceBusSessionReceiver> Next()
        {
            if (_next == sessions.Length) throw new ServiceBusException("No available sessions", ServiceBusFailureReason.ServiceTimeout);
            var receiver = new SessionReceiver(sessions[_next++]); Receivers.Add(receiver);
            return Task.FromResult<ServiceBusSessionReceiver>(receiver);
        }
        public override Task<ServiceBusSessionReceiver> AcceptNextSessionAsync(string queueName, ServiceBusSessionReceiverOptions? options = null,
            CancellationToken cancellationToken = default) => Next();
        public override Task<ServiceBusSessionReceiver> AcceptNextSessionAsync(string topicName, string subscriptionName,
            ServiceBusSessionReceiverOptions? options = null, CancellationToken cancellationToken = default) => Next();
    }

    private sealed class SessionReceiver(ServiceBusReceivedMessage[] messages) : ServiceBusSessionReceiver
    {
        public bool Disposed { get; private set; }
        public override Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekMessagesAsync(int maxMessages, long? fromSequenceNumber = null,
            CancellationToken cancellationToken = default) => Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(
                messages.Where(m => m.SequenceNumber >= (fromSequenceNumber ?? 0)).Take(maxMessages).ToArray());
        public override ValueTask DisposeAsync() { Disposed = true; return ValueTask.CompletedTask; }
    }
}
