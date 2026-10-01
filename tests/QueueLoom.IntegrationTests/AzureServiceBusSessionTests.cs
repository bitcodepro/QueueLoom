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
    public async Task Dead_letters_of_a_session_queue_can_be_searched_deleted_and_moved_back()
    {
        await DeadLetterAsync(SessionQueue, null, "cart-7", "order 1042 failed");
        await DeadLetterAsync(SessionQueue, null, "cart-8", "order 1043 failed");
        var queue = ServiceBusEntityReference.Queue(SessionQueue);

        var dead = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(queue, ServiceBusSubQueue.DeadLetter));
        Assert.Equal(2, dead.Count);
        var search = await _workspace.SearchDeadLettersAsync(new DeadLetterSearchRequest("1042",
            [new DeadLetterSearchTarget(queue, ServiceBusSubQueue.DeadLetter, 2)]));
        var match = Assert.Single(search.Matches);

        var move = await DeadLetterResender.ResendAsync(_workspace,
            [new ResendItem(match, queue, match.CreateDraft())], ResendMode.Move);
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
