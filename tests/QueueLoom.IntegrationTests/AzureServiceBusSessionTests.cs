using Azure.Messaging.ServiceBus;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;
using AzureMessage = Azure.Messaging.ServiceBus.ServiceBusMessage;

namespace QueueLoom.IntegrationTests;

/// <summary>
/// Session-enabled entities against the Azure Service Bus emulator, started with ServiceBusEmulatorConfig.json:
/// the queue "checkout-sessions" and the topic "customer-events" with the subscription "per-customer" use sessions.
/// </summary>
public sealed class AzureServiceBusSessionTests : IAsyncLifetime
{
    private const string SessionQueue = "checkout-sessions";
    private const string Topic = "customer-events";
    private const string SessionSubscription = "per-customer";
    private readonly TemporaryDirectory _directory = new();
    private ServiceBusClient _client = null!;
    private AzureServiceBusWorkspace _workspace = null!;

    public async ValueTask InitializeAsync()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(Emulators.ServiceBus)))
        {
            return;
        }

        _client = new ServiceBusClient(Emulators.ServiceBusConnectionString);
        await DrainAsync(SessionQueue, null);
        await DrainAsync(Topic, SessionSubscription);

        var vault = new InMemorySecretVault();
        var profile = ServiceBusProfile.CreateNew(
            "Emulator", EnvironmentKind.Development, AuthenticationSettings.ConnectionString(), accessMode: ProfileAccessMode.ReadWrite);
        await vault.StoreAsync(ProfileSecretKey.ConnectionString(profile.Id), Emulators.ServiceBusConnectionString);
        _workspace = new AzureServiceBusWorkspace(vault, backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(_directory.Path)));
        await _workspace.ConnectAsync(profile);
        await _workspace.GetTopologyAsync(forceRefresh: true);
    }

    public async ValueTask DisposeAsync()
    {
        if (_workspace is not null)
        {
            await _workspace.DisposeAsync();
        }
        if (_client is not null)
        {
            await _client.DisposeAsync();
        }
        _directory.Dispose();
    }

    [EmulatorFact(Emulators.ServiceBus)]
    public async Task Active_messages_of_a_session_queue_are_peeked_across_sessions_and_stay_in_place()
    {
        var queue = ServiceBusEntityReference.Queue(SessionQueue);
        foreach (var (session, body) in new[] { ("cart-1", "a"), ("cart-1", "b"), ("cart-2", "c") })
        {
            await _workspace.SendMessageAsync(new SendMessageRequest(queue, new MessageDraft(
                new EditableMessageBody(body, MessageBodyFormat.Text), new EditableMessageProperties(SessionId: session))));
        }

        var first = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(queue));
        var second = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(queue));

        Assert.Equal(["a", "b", "c"], first.Select(message => message.CreateDraft().Body.Content));
        Assert.Equal(["cart-1", "cart-1", "cart-2"], first.Select(message => message.Properties.SessionId));
        Assert.Equal(3, second.Count);
        Assert.All(first, message => Assert.Equal(0, message.DeliveryCount));
    }

    [EmulatorFact(Emulators.ServiceBus)]
    public Task SessionQueuePagesInterleavedMessagesWithoutSkipping() => VerifyPagesAsync(SessionQueue, null);

    [EmulatorFact(Emulators.ServiceBus)]
    public Task SessionSubscriptionPagesInterleavedMessagesWithoutSkipping() => VerifyPagesAsync(Topic, SessionSubscription);

    private async Task VerifyPagesAsync(string entity, string? subscription)
    {
        var source = subscription is null ? ServiceBusEntityReference.Queue(entity) : ServiceBusEntityReference.Subscription(entity, subscription);
        await using var sender = _client.CreateSender(entity);
        var messages = Enumerable.Range(1, 202).Select(i => new AzureMessage($"page-{i}")
            { MessageId = Guid.NewGuid().ToString("N"), SessionId = i % 2 == 0 ? "audit-even" : "audit-odd" }).ToArray();
        await sender.SendMessagesAsync(messages);
        var result = new List<BrowsedMessage>();
        long? cursor = null;
        for (var page = 0; page < 3; page++)
        {
            var batch = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(source, maxMessages: 100, fromSequenceNumber: cursor));
            Assert.Equal(page == 2 ? 2 : 100, batch.Count);
            result.AddRange(batch);
            cursor = batch.Max(m => m.SequenceNumber) + 1;
        }
        Assert.Equal(202, result.Select(m => m.SequenceNumber).Distinct().Count());
        Assert.Equal(Enumerable.Range(1, 202).Select(i => $"page-{i}"), result.Select(m => m.CreateDraft().Body.Content));
    }

    [EmulatorFact(Emulators.ServiceBus)]
    public async Task Dead_letters_of_a_session_queue_can_be_searched_deleted_and_moved_back()
    {
        await DeadLetterAsync(SessionQueue, null, "cart-7", "order 1042 failed");
        await DeadLetterAsync(SessionQueue, null, "cart-8", "order 1043 failed");
        var queue = ServiceBusEntityReference.Queue(SessionQueue);

        var dead = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(queue, ServiceBusSubQueue.DeadLetter));
        Assert.Equal(2, dead.Count);
        // The term has a space: a bare number can also occur in a random Message ID.
        var search = await _workspace.SearchDeadLettersAsync(new DeadLetterSearchRequest("order 1042",
            [new DeadLetterSearchTarget(queue, ServiceBusSubQueue.DeadLetter, 2)]));
        var match = Assert.Single(search.Matches);

        var move = await DeadLetterResender.ResendAsync(_workspace,
            [new ResendItem(match, queue, match.CreateDraft()).WithNewMessageId()], ResendMode.Move);
        Assert.Equal(ResendOutcome.Moved, Assert.Single(move.Items).Outcome);

        var remaining = Assert.Single(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(queue, ServiceBusSubQueue.DeadLetter)));
        Assert.Equal("order 1043 failed", remaining.CreateDraft().Body.Content);
        var active = Assert.Single(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(queue)));
        Assert.Equal("cart-7", active.Properties.SessionId);

        var deletion = await _workspace.DeleteDeadLetterMessagesAsync(new DeleteDeadLetterMessagesRequest(
            [new DeadLetterMessageKey(queue, ServiceBusSubQueue.DeadLetter, remaining.SequenceNumber, remaining.Properties.MessageId)]));
        Assert.Equal(1, deletion.DeletedCount);
        Assert.Empty(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(queue, ServiceBusSubQueue.DeadLetter)));
    }

    [EmulatorFact(Emulators.ServiceBus)]
    public async Task A_session_subscription_can_be_browsed_and_purged()
    {
        var subscription = ServiceBusEntityReference.Subscription(Topic, SessionSubscription);
        await _workspace.SendMessageAsync(new SendMessageRequest(ServiceBusEntityReference.Topic(Topic), new MessageDraft(
            new EditableMessageBody("customer 77 updated", MessageBodyFormat.Text), new EditableMessageProperties(SessionId: "C-77"))));
        await DeadLetterAsync(Topic, SessionSubscription, "C-78", "customer 78 failed");

        Assert.Equal("customer 77 updated",
            Assert.Single(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(subscription))).CreateDraft().Body.Content);
        var purge = await _workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest([subscription], [ServiceBusSubQueue.DeadLetter]));

        Assert.False(purge.HasFailures);
        Assert.Equal(1, purge.DeletedCount);
        Assert.Empty(await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(subscription, ServiceBusSubQueue.DeadLetter)));
    }

    /// <summary>
    /// Service Bus hands out only sessions that have an active message: a session whose messages were all deferred is not
    /// accepted, so a browse cannot list them (the workspace reports the gap from the entity's counts, which the emulator
    /// does not have). Guards that assumption; the deferred messages stay reachable by session id and sequence number.
    /// </summary>
    [EmulatorFact(Emulators.ServiceBus)]
    public async Task A_session_holding_only_deferred_messages_is_not_handed_out_to_a_browse()
    {
        var idle = "deferred-only-" + Guid.NewGuid().ToString("N")[..8];
        await using (var sender = _client.CreateSender(SessionQueue))
        {
            await sender.SendMessagesAsync([new AzureMessage("deferred 1") { SessionId = idle }, new AzureMessage("deferred 2") { SessionId = idle },
                new AzureMessage("active") { SessionId = "busy" }]);
        }
        var deferred = new List<long>();
        await using (var receiver = await _client.AcceptSessionAsync(SessionQueue, idle))
        {
            foreach (var message in await receiver.ReceiveMessagesAsync(2, TimeSpan.FromSeconds(10)))
            {
                await receiver.DeferMessageAsync(message);
                deferred.Add(message.SequenceNumber);
            }
        }
        Assert.Equal(2, deferred.Count);
        try
        {
            var browsed = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue(SessionQueue)));

            Assert.Equal("busy", Assert.Single(browsed).Properties.SessionId);
            await using var next = await _client.AcceptNextSessionAsync(SessionQueue);
            Assert.Equal("busy", next.SessionId);
            using var wait = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await Assert.ThrowsAnyAsync<Exception>(async () => await _client.AcceptNextSessionAsync(SessionQueue, cancellationToken: wait.Token));
        }
        finally
        {
            await using var receiver = await _client.AcceptSessionAsync(SessionQueue, idle);
            Assert.Equal([Azure.Messaging.ServiceBus.ServiceBusMessageState.Deferred, Azure.Messaging.ServiceBus.ServiceBusMessageState.Deferred], (await receiver.PeekMessagesAsync(10)).Select(m => m.State));
            foreach (var message in await receiver.ReceiveDeferredMessagesAsync(deferred))
            {
                await receiver.CompleteMessageAsync(message);
            }
        }
    }

    /// <summary>
    /// A session whose messages are all deferred is not handed out to a browse, but opening it by id peeks every message
    /// in it as Deferred, leaves them in place and releases the session for its consumers.
    /// </summary>
    [EmulatorFact(Emulators.ServiceBus)]
    public async Task A_session_holding_only_deferred_messages_is_listed_when_opened_by_id()
    {
        var idle = "opened-by-id-" + Guid.NewGuid().ToString("N")[..8];
        await using (var sender = _client.CreateSender(SessionQueue))
        {
            await sender.SendMessagesAsync([new AzureMessage("deferred 1") { SessionId = idle }, new AzureMessage("deferred 2") { SessionId = idle }]);
        }
        var deferred = new List<long>();
        await using (var receiver = await _client.AcceptSessionAsync(SessionQueue, idle))
        {
            foreach (var message in await receiver.ReceiveMessagesAsync(2, TimeSpan.FromSeconds(10)))
            {
                await receiver.DeferMessageAsync(message);
                deferred.Add(message.SequenceNumber);
            }
        }
        Assert.Equal(2, deferred.Count);
        try
        {
            var browsed = await _workspace.BrowseMessagesAsync(
                new BrowseMessagesRequest(ServiceBusEntityReference.Queue(SessionQueue)) { SessionId = idle });

            Assert.Equal(deferred.Order(), browsed.Select(message => message.SequenceNumber));
            Assert.All(browsed, message =>
            {
                Assert.Equal(QueueLoom.Core.ServiceBus.ServiceBusMessageState.Deferred, message.State);
                Assert.Equal(idle, message.Properties.SessionId);
            });
            Assert.Equal(["deferred 1", "deferred 2"], browsed.Select(message => System.Text.Encoding.UTF8.GetString(message.Body.Span)));
        }
        finally
        {
            // The browse released the session at once, and both messages are still there to be received.
            await using var receiver = await _client.AcceptSessionAsync(SessionQueue, idle);
            var received = await receiver.ReceiveDeferredMessagesAsync(deferred);
            Assert.Equal(2, received.Count);
            foreach (var message in received)
            {
                await receiver.CompleteMessageAsync(message);
            }
        }
    }

    private async Task DeadLetterAsync(string entity, string? subscription, string session, string body)
    {
        await using (var sender = _client.CreateSender(entity))
        {
            await sender.SendMessageAsync(new AzureMessage(body) { SessionId = session });
        }

        await using var receiver = subscription is null
            ? await _client.AcceptSessionAsync(entity, session)
            : await _client.AcceptSessionAsync(entity, subscription, session);
        var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10))
            ?? throw new InvalidOperationException("The message did not arrive.");
        await receiver.DeadLetterMessageAsync(message, "TestFailure", "Dead-lettered by the test.");
    }

    /// <summary>The emulator's entities are shared by every test: empty them first.</summary>
    private async Task DrainAsync(string entity, string? subscription)
    {
        var deadLetterOptions = new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter, ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete };
        await using (var receiver = subscription is null
                         ? _client.CreateReceiver(entity, deadLetterOptions)
                         : _client.CreateReceiver(entity, subscription, deadLetterOptions))
        {
            while ((await receiver.ReceiveMessagesAsync(100, TimeSpan.FromSeconds(1))).Count > 0)
            {
            }
        }

        var sessionOptions = new ServiceBusSessionReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete };
        while (true)
        {
            using var wait = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            ServiceBusSessionReceiver session;
            try
            {
                session = subscription is null
                    ? await _client.AcceptNextSessionAsync(entity, sessionOptions, wait.Token)
                    : await _client.AcceptNextSessionAsync(entity, subscription, sessionOptions, wait.Token);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (ServiceBusException exception) when (exception.Reason == ServiceBusFailureReason.ServiceTimeout)
            {
                return;
            }

            await using (session)
            {
                while ((await session.ReceiveMessagesAsync(100, TimeSpan.FromSeconds(1))).Count > 0)
                {
                }
            }
        }
    }
}
