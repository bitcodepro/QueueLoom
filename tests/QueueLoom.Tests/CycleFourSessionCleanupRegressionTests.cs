using System.Reflection;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

public sealed class CycleFourSessionCleanupRegressionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CleanupFailureStillAttemptsEveryAcceptedSession(bool subscription)
    {
        var firstError = new IOException("first session close failed");
        var lastError = new IOException("last session close failed");
        var receivers = new[] { new Receiver(1, closeError: firstError), new Receiver(2), new Receiver(3, closeError: lastError) };
        await using var owner = Owner(subscription, new Client(receivers));

        var error = await Record.ExceptionAsync(() => owner.BrowseMessagesAsync(Request(subscription)));

        Assert.All(receivers, receiver => Assert.Equal(1, receiver.CloseAttempts));
        var aggregate = Assert.IsType<AggregateException>(error);
        Assert.Equal(new Exception[] { firstError, lastError }, aggregate.InnerExceptions);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PeekFailureSurvivesCleanupFailureAndOtherSessionsAreReleased(bool subscription)
    {
        var primary = new ServiceBusException("session lock lost while peeking", ServiceBusFailureReason.SessionLockLost);
        var cleanup = new IOException("close failed");
        var receivers = new[] { new Receiver(1, closeError: cleanup), new Receiver(2, peekError: primary) };
        await using var owner = Owner(subscription, new Client(receivers));

        var error = await Record.ExceptionAsync(() => owner.BrowseMessagesAsync(Request(subscription)));

        Assert.Same(primary, error);
        Assert.All(receivers, receiver => Assert.Equal(1, receiver.CloseAttempts));
        Assert.Same(cleanup, Assert.IsType<AggregateException>(primary.Data["SessionCleanupErrors"]).InnerExceptions.Single());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CancellationSurvivesCleanupFailureAndTheNextBrowseWorks(bool subscription)
    {
        using var stop = new CancellationTokenSource();
        var first = new Receiver(1, closeError: new IOException("close failed"));
        var cancelled = new Receiver(2, beforePeek: () => stop.Cancel());
        var retry = new Receiver(3);
        var client = new Client([first, cancelled]);
        await using var owner = Owner(subscription, client);

        var error = await Record.ExceptionAsync(() => owner.BrowseMessagesAsync(Request(subscription), stop.Token));

        var cancellation = Assert.IsAssignableFrom<OperationCanceledException>(error);
        Assert.Equal(stop.Token, cancellation.CancellationToken);
        Assert.Equal(1, first.CloseAttempts);
        Assert.Equal(1, cancelled.CloseAttempts);
        client.Reset([retry]);
        var messages = await owner.BrowseMessagesAsync(Request(subscription));
        Assert.Equal(3L, Assert.Single(messages).SequenceNumber);
        Assert.Equal(1, retry.CloseAttempts);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SessionLimitErrorSurvivesCleanupFailureAndReleasesAllLocks(bool subscription)
    {
        var receivers = Enumerable.Range(1, 101).Select(i => new Receiver(i,
            closeError: i == 1 ? new IOException("first close failed") : null)).ToArray();
        await using var owner = Owner(subscription, new Client(receivers));

        var error = await Record.ExceptionAsync(() => owner.BrowseMessagesAsync(Request(subscription)));

        Assert.Contains("Global page order", Assert.IsType<InvalidOperationException>(error).Message);
        Assert.All(receivers, receiver => Assert.Equal(1, receiver.CloseAttempts));
    }

    [Fact]
    public async Task DisconnectWaitsForEveryCleanupAttemptEvenAfterAFailedClose()
    {
        var closeStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseClose = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = new Receiver(1, closeError: new IOException("close failed"), closeGate: async () =>
        {
            closeStarted.TrySetResult();
            await releaseClose.Task;
        });
        var second = new Receiver(2);
        var client = new Client([first, second]);
        await using var owner = Owner(false, client);
        var browse = owner.BrowseMessagesAsync(Request(false));
        await closeStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        var disconnect = owner.DisconnectAsync();
        try
        {
            Assert.False(disconnect.IsCompleted);
            Assert.Equal(0, client.CloseAttempts);
        }
        finally { releaseClose.TrySetResult(); }

        _ = await Record.ExceptionAsync(() => browse);
        await disconnect.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(1, second.CloseAttempts);
        Assert.Equal(1, client.CloseAttempts);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SuccessfulCleanupKeepsGlobalOrderAndAllowsRepeatedBrowse(bool subscription)
    {
        var first = new Receiver(2);
        var second = new Receiver(1);
        var client = new Client([first, second]);
        await using var owner = Owner(subscription, client);
        var messages = await owner.BrowseMessagesAsync(Request(subscription));
        Assert.Equal(new long[] { 1, 2 }, messages.Select(message => message.SequenceNumber));
        Assert.All(new[] { first, second }, receiver => Assert.Equal(1, receiver.CloseAttempts));
        var retry = new Receiver(3);
        client.Reset([retry]);
        Assert.Equal(3L, Assert.Single(await owner.BrowseMessagesAsync(Request(subscription))).SequenceNumber);
        Assert.Equal(1, retry.CloseAttempts);
    }

    private static BrowseMessagesRequest Request(bool subscription) => new(subscription
        ? ServiceBusEntityReference.Subscription("events", "billing") : ServiceBusEntityReference.Queue("orders"), maxMessages: 2);

    private static AzureServiceBusWorkspace Owner(bool subscription, Client client)
    {
        var owner = new AzureServiceBusWorkspace(new DeepAuditCloudTests.EmptyVault());
        Set(owner, "_client", client);
        Set(owner, "_cachedTopology", subscription
            ? new ServiceBusTopology(DateTimeOffset.UtcNow, topics: [new ServiceBusTopic("events", ServiceBusEntityRuntime.Empty,
                subscriptions: [new ServiceBusSubscription("events", "billing", ServiceBusEntityRuntime.Empty, RequiresSession: true)])])
            : new ServiceBusTopology(DateTimeOffset.UtcNow, [new ServiceBusQueue("orders", ServiceBusEntityRuntime.Empty, RequiresSession: true)]));
        return owner;
    }

    private static void Set(object owner, string name, object value) => owner.GetType()
        .GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner, value);

    private sealed class Client(Receiver[] receivers) : ServiceBusClient
    {
        private Receiver[] _receivers = receivers;
        private int _next;
        public int CloseAttempts { get; private set; }
        public void Reset(Receiver[] next) { _receivers = next; _next = 0; }
        public override ValueTask DisposeAsync() { CloseAttempts++; return ValueTask.CompletedTask; }
        private Task<ServiceBusSessionReceiver> Next(CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            if (_next == _receivers.Length)
                throw new ServiceBusException("No available sessions", ServiceBusFailureReason.ServiceTimeout);
            return Task.FromResult<ServiceBusSessionReceiver>(_receivers[_next++]);
        }
        public override Task<ServiceBusSessionReceiver> AcceptNextSessionAsync(string queueName, ServiceBusSessionReceiverOptions? options = null,
            CancellationToken cancellationToken = default) => Next(cancellationToken);
        public override Task<ServiceBusSessionReceiver> AcceptNextSessionAsync(string topicName, string subscriptionName,
            ServiceBusSessionReceiverOptions? options = null, CancellationToken cancellationToken = default) => Next(cancellationToken);
    }

    private sealed class Receiver(long sequence, Exception? closeError = null, Exception? peekError = null,
        Action? beforePeek = null, Func<Task>? closeGate = null) : ServiceBusSessionReceiver
    {
        public int CloseAttempts { get; private set; }
        public override Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekMessagesAsync(int maxMessages, long? fromSequenceNumber = null,
            CancellationToken cancellationToken = default)
        {
            beforePeek?.Invoke();
            cancellationToken.ThrowIfCancellationRequested();
            if (peekError is not null) throw peekError;
            return Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(fromSequenceNumber > sequence ? [] :
                [ServiceBusModelFactory.ServiceBusReceivedMessage(body: BinaryData.FromString("{}"), messageId: $"m-{sequence}", sequenceNumber: sequence)]);
        }
        public override async ValueTask DisposeAsync()
        {
            CloseAttempts++;
            if (closeGate is not null) await closeGate();
            if (closeError is not null) throw closeError;
        }
    }
}
