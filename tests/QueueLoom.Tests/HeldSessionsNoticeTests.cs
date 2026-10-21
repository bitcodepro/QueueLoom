using System.Reflection;
using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

// Service Bus hands out only sessions that no consumer holds, so browsing a session queue left out the messages of held
// sessions without a word. When the entity's active count is higher than what every readable session held, the browse
// now says that some sessions could not be read.
public sealed class HeldSessionsNoticeTests
{
    [Fact]
    public async Task MessagesInHeldSessionsAreAnnounced()
    {
        await using var workspace = Workspace(active: 5, sessions: [1, 2]);
        var notices = Collect(workspace);

        var messages = await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue("orders"), maxMessages: 50));

        Assert.Equal(2, messages.Count);
        var notice = Assert.Single(notices);
        Assert.Equal("Session browse may be incomplete", notice.Title);
        Assert.Contains("reports 5 active messages, but this browse read 2", notice.Details, StringComparison.Ordinal);
        Assert.Contains("another consumer holds some sessions", notice.Details, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MessagesThatArriveDuringTheBrowseAreNamedAsAPossibleReasonNotDeniedAsHeldSessions()
    {
        // Nobody else holds a session: a producer added three messages to session 1 after it had been peeked, so the
        // runtime count read afterwards is ahead of the browse. The notice reports the discrepancy and names arrivals
        // as a reason; it does not claim that another consumer holds a session.
        long active = 2;
        await using var workspace = Workspace(() => active, sessions: [1, 2], afterPeek: sequence => { if (sequence == 1) active += 3; });
        var notices = Collect(workspace);

        var messages = await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue("orders"), maxMessages: 50));

        Assert.Equal(2, messages.Count);
        var notice = Assert.Single(notices);
        Assert.Contains("reports 5 active messages, but this browse read 2", notice.Details, StringComparison.Ordinal);
        Assert.Contains("messages arrived while browsing", notice.Details, StringComparison.Ordinal);
        Assert.DoesNotContain("could not be browsed", notice.Title + notice.Details, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(1)]
    public async Task NothingIsSaidWhenEveryMessageWasRead(long active)
    {
        await using var workspace = Workspace(active, sessions: [1, 2]);
        var notices = Collect(workspace);

        await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue("orders"), maxMessages: 50));

        Assert.Empty(notices);
    }

    [Fact]
    public async Task APageThatStoppedAtItsLimitCannotTellAndSaysNothing()
    {
        // One message per session is all a page of one may read; the rest is unread, not held.
        await using var workspace = Workspace(active: 5, sessions: [1, 2]);
        var notices = Collect(workspace);

        await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue("orders"), maxMessages: 1));

        Assert.Empty(notices);
    }

    [Fact]
    public async Task WithoutManagementRightsTheBrowseStillSucceedsSilently()
    {
        await using var workspace = Workspace(active: null, sessions: [1]);
        var notices = Collect(workspace);

        var messages = await workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue("orders"), maxMessages: 50));

        Assert.Single(messages);
        Assert.Empty(notices);
    }

    private static List<WorkspaceNotice> Collect(AzureServiceBusWorkspace workspace)
    {
        var notices = new List<WorkspaceNotice>();
        ((IWorkspaceNoticeSource)workspace).Notice += (_, notice) => notices.Add(notice);
        return notices;
    }

    private static AzureServiceBusWorkspace Workspace(long? active, long[] sessions) => Workspace(() => active, sessions);

    private static AzureServiceBusWorkspace Workspace(Func<long?> active, long[] sessions, Action<long>? afterPeek = null)
    {
        var workspace = new AzureServiceBusWorkspace(new DeepAuditCloudTests.EmptyVault());
        Set(workspace, "_client", new Client(sessions.Select(sequence => new Receiver(sequence, afterPeek)).ToArray()));
        Set(workspace, "_administration", new Administration(active));
        Set(workspace, "_cachedTopology", new ServiceBusTopology(DateTimeOffset.UtcNow,
            [new ServiceBusQueue("orders", ServiceBusEntityRuntime.Empty, RequiresSession: true)]));
        return workspace;
    }

    private static void Set(object owner, string name, object value) => owner.GetType()
        .GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(owner, value);

    /// <summary>The queue's runtime properties with the given active count; null refuses them, as without Manage rights.</summary>
    private sealed class Administration(Func<long?> active) : ServiceBusAdministrationClient
    {
        public override Task<Response<QueueRuntimeProperties>> GetQueueRuntimePropertiesAsync(string name, CancellationToken cancellationToken = default) =>
            active() is { } count
                ? Task.FromResult(Response.FromValue(ServiceBusModelFactory.QueueRuntimeProperties(name, activeMessageCount: count), null!))
                : Task.FromException<Response<QueueRuntimeProperties>>(new UnauthorizedAccessException("Manage claim required"));
    }

    private sealed class Client(Receiver[] receivers) : ServiceBusClient
    {
        private int _next;
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public override Task<ServiceBusSessionReceiver> AcceptNextSessionAsync(string queueName, ServiceBusSessionReceiverOptions? options = null,
            CancellationToken cancellationToken = default) => _next == receivers.Length
            ? throw new ServiceBusException("No available sessions", ServiceBusFailureReason.ServiceTimeout)
            : Task.FromResult<ServiceBusSessionReceiver>(receivers[_next++]);
    }

    private sealed class Receiver(long sequence, Action<long>? afterPeek) : ServiceBusSessionReceiver
    {
        public override Task<IReadOnlyList<ServiceBusReceivedMessage>> PeekMessagesAsync(int maxMessages, long? fromSequenceNumber = null,
            CancellationToken cancellationToken = default)
        {
            if (fromSequenceNumber > sequence) return Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>([]);
            afterPeek?.Invoke(sequence);
            return Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(
                [ServiceBusModelFactory.ServiceBusReceivedMessage(body: BinaryData.FromString("{}"), messageId: $"m-{sequence}", sequenceNumber: sequence)]);
        }
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
