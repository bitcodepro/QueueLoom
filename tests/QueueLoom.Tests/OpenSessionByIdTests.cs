using System.Reflection;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using AzureMessageState = Azure.Messaging.ServiceBus.ServiceBusMessageState;
using DomainMessageState = QueueLoom.Core.ServiceBus.ServiceBusMessageState;

namespace QueueLoom.Tests;

// Service Bus never hands a session whose messages are all deferred to AcceptNextSessionAsync, so a browse cannot list
// them. A browse that names a session id accepts that session directly and peeks every message in it, deferred ones too.
public sealed class OpenSessionByIdTests
{
    [Fact]
    public async Task ANamedSessionIsAcceptedDirectlyPeekedAndReleased()
    {
        var receiver = new Receiver([(1, AzureMessageState.Deferred), (2, AzureMessageState.Deferred), (3, AzureMessageState.Scheduled)]);
        var client = new Client(receiver);
        await using var workspace = Workspace(client, requiresSession: true);

        var messages = await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue("orders")) { SessionId = "idle" });

        Assert.Equal(["idle"], client.AcceptedSessions);
        Assert.Equal(0, client.AcceptNextCalls);
        Assert.Equal([1L, 2L, 3L], messages.Select(message => message.SequenceNumber));
        Assert.Equal([DomainMessageState.Deferred, DomainMessageState.Deferred, DomainMessageState.Scheduled], messages.Select(message => message.State));
        Assert.True(receiver.Disposed);
    }

    [Fact]
    public async Task APaddedSessionIdIsOpenedExactlyAndNotConfusedWithTheUnpaddedOne()
    {
        var padded = new Receiver([(7, AzureMessageState.Deferred)]);
        var plain = new Receiver([(1, AzureMessageState.Active)]);
        var client = new Client(null, new Dictionary<string, Receiver> { [" customer-7 "] = padded, ["customer-7"] = plain });
        await using var workspace = Workspace(client, requiresSession: true);

        var messages = await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue("orders")) { SessionId = " customer-7 " });

        Assert.Equal([" customer-7 "], client.AcceptedSessions);
        Assert.Equal(7L, Assert.Single(messages).SequenceNumber);
        Assert.True(padded.Disposed);
        Assert.False(plain.Disposed);
    }

    [Fact]
    public async Task ASessionHeldByAnotherConsumerIsReportedPlainly()
    {
        var client = new Client(null);
        await using var workspace = Workspace(client, requiresSession: true);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue("orders")) { SessionId = "busy" }));

        Assert.Contains("Session 'busy'", error.Message, StringComparison.Ordinal);
        Assert.Contains("held by another consumer", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnEntityWithoutSessionsCannotBeOpenedBySessionId()
    {
        var client = new Client(new Receiver([]));
        await using var workspace = Workspace(client, requiresSession: false);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue("orders")) { SessionId = "idle" }));

        Assert.Contains("does not use sessions", error.Message, StringComparison.Ordinal);
        Assert.Empty(client.AcceptedSessions);
    }

    private static AzureServiceBusWorkspace Workspace(Client client, bool requiresSession)
    {
        var workspace = new AzureServiceBusWorkspace(new DeepAuditCloudTests.EmptyVault());
        Set(workspace, "_client", client);
        Set(workspace, "_cachedTopology", new ServiceBusTopology(DateTimeOffset.UtcNow,
            [new ServiceBusQueue("orders", ServiceBusEntityRuntime.Empty, RequiresSession: requiresSession)]));
        return workspace;
    }

    private static void Set(object owner, string name, object value) => owner.GetType()
        .GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner, value);

    /// <summary>
    /// Hands out <paramref name="receiver"/> for any session id, or the receiver keyed by the exact id; null refuses it as a
    /// session locked elsewhere.
    /// </summary>
    private sealed class Client(Receiver? receiver, IReadOnlyDictionary<string, Receiver>? bySessionId = null) : ServiceBusClient
    {
        public List<string> AcceptedSessions { get; } = [];
        public int AcceptNextCalls { get; private set; }
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public override Task<ServiceBusSessionReceiver> AcceptSessionAsync(string queueName, string sessionId,
            ServiceBusSessionReceiverOptions? options = null, CancellationToken cancellationToken = default)
        {
            AcceptedSessions.Add(sessionId);
            var match = bySessionId is null ? receiver : bySessionId.GetValueOrDefault(sessionId);
            return match is null
                ? throw new ServiceBusException("The session lock was not acquired", ServiceBusFailureReason.SessionCannotBeLocked)
                : Task.FromResult<ServiceBusSessionReceiver>(match);
        }
        public override Task<ServiceBusSessionReceiver> AcceptNextSessionAsync(string queueName, ServiceBusSessionReceiverOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            AcceptNextCalls++;
            throw new ServiceBusException("No available sessions", ServiceBusFailureReason.ServiceTimeout);
        }
    }

    private sealed class Receiver((long Sequence, AzureMessageState State)[] messages) : ServiceBusSessionReceiver
    {
        public bool Disposed { get; private set; }
        public override Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekMessagesAsync(int maxMessages, long? fromSequenceNumber = null,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(messages
                .Where(message => message.Sequence >= (fromSequenceNumber ?? 0))
                .Take(maxMessages)
                .Select(message => ServiceBusModelFactory.ServiceBusReceivedMessage(body: BinaryData.FromString("{}"),
                    messageId: $"m-{message.Sequence}", sessionId: "idle", sequenceNumber: message.Sequence, serviceBusMessageState: message.State))
                .ToArray());
        public override ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }
}
