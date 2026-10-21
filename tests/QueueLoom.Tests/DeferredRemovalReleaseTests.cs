using System.Reflection;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;
using ServiceBusMessageState = QueueLoom.Core.ServiceBus.ServiceBusMessageState;

namespace QueueLoom.Tests;

// Removing a deferred message receives it by sequence number. When that number now belongs to another message, the
// receive had locked that other message and left it locked until the lock expired, so its own receiver could not take
// it meanwhile; it is now released at once. When the backup fails, the lock is released too, but a failure to release
// it replaced the backup error the operator needs; the backup error is now the one reported.
public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task DeferredRemoval_ReleasesAMessageThatIsNotTheOneChosen()
    {
        using var directory = new TemporaryDirectory();
        var client = new DeferredClient(receivedMessageId: "someone-else");
        await using var azure = DeferredWorkspace(client, QueueLoomPaths.ForRoot(directory.Path));

        var result = await azure.RemovePendingMessagesAsync([Deferred(7)], CancellationToken.None);

        Assert.True(result.Messages[0].Outcome == DeadLetterMessageDeletionOutcome.NotFound, result.Messages[0].Detail);
        Assert.Equal([7L], client.Abandoned);
        Assert.Empty(client.Completed);
    }

    [Fact]
    public async Task DeferredRemoval_ReportsTheBackupErrorEvenWhenReleasingFails()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        // When the message is received the backup session exists; a file put where the message's backup folder goes
        // makes that one backup fail.
        var client = new DeferredClient(receivedMessageId: "m-7")
        {
            AbandonFails = true,
            OnReceive = () =>
            {
                var segment = DeadLetterJsonBackupStore.UniqueEntitySegment(ServiceBusEntityReference.Queue("orders"));
                foreach (var folder in Directory.GetDirectories(paths.BackupsDirectory, "*", SearchOption.AllDirectories).Append(paths.BackupsDirectory))
                {
                    var blocked = Path.Combine(folder, segment);
                    if (!Directory.Exists(blocked)) File.WriteAllText(blocked, "not a folder");
                }
            }
        };
        await using var azure = DeferredWorkspace(client, paths);

        string detail;
        try
        {
            var result = await azure.RemovePendingMessagesAsync([Deferred(7)], CancellationToken.None);
            detail = $"{result.Messages[0].Outcome}: {result.Messages[0].Detail}";
        }
        catch (Exception error) { detail = $"{error.GetType().Name}: {error.Message}"; }
        Assert.True(client.Abandoned.Count == 1, detail);
        Assert.DoesNotContain("abandon refused", detail, StringComparison.Ordinal);
        Assert.Equal([7L], client.Abandoned);
        Assert.Empty(client.Completed);
    }

    // A failed completion does not say whether the broker deleted the message (an SDK timeout can come after it did):
    // it was reported as a plain failure with the SDK's text, and the message, if still there, stayed locked.
    [Theory]
    [InlineData("timeout")]
    [InlineData("busy")]
    public async Task DeferredRemoval_AnUnconfirmedCompletionKeepsTheBackupAndReleasesTheMessage(string failure)
    {
        using var directory = new TemporaryDirectory();
        var client = new DeferredClient(receivedMessageId: "m-7")
        {
            CompleteFailure = failure == "timeout"
                ? new TaskCanceledException("The operation did not complete within the allocated time.")
                : new ServiceBusException("The server is busy.", ServiceBusFailureReason.ServiceBusy)
        };
        await using var azure = DeferredWorkspace(client, QueueLoomPaths.ForRoot(directory.Path));

        var result = await azure.RemovePendingMessagesAsync([Deferred(7)], CancellationToken.None);

        var removal = Assert.Single(result.Messages);
        Assert.Equal(DeadLetterMessageDeletionOutcome.Failed, removal.Outcome);
        Assert.Contains("outcome is unknown", removal.Detail, StringComparison.Ordinal);
        Assert.Contains("The backup is kept", removal.Detail, StringComparison.Ordinal);
        Assert.Equal([7L], client.Abandoned);
        Assert.Equal([7L], client.Completed);
    }

    private static BrowsedMessage Deferred(long number) => new(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.Active, number,
        "pending"u8.ToArray(), new EditableMessageProperties(MessageId: $"m-{number}"), state: ServiceBusMessageState.Deferred);

    private static AzureServiceBusWorkspace DeferredWorkspace(DeferredClient client, QueueLoomPaths paths)
    {
        var azure = new AzureServiceBusWorkspace(new FakeSecretVault(), backupStore: new DeadLetterJsonBackupStore(paths));
        typeof(AzureServiceBusWorkspace).GetField("_client", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure, client);
        typeof(AzureServiceBusWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure,
            CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite));
        typeof(AzureServiceBusWorkspace).GetField("_cachedTopology", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure,
            new ServiceBusTopology(DateTimeOffset.UtcNow, [new ServiceBusQueue("orders", new ServiceBusEntityRuntime(new ServiceBusMessageCounts(active: 1)))]));
        return azure;
    }

    private sealed class DeferredClient(string receivedMessageId) : ServiceBusClient
    {
        public List<long> Abandoned { get; } = [];
        public List<long> Completed { get; } = [];
        public bool AbandonFails { get; init; }
        public Exception? CompleteFailure { get; init; }
        public Action? OnReceive { get; init; }
        public string ReceivedMessageId => receivedMessageId;
        public override ServiceBusReceiver CreateReceiver(string queueName, ServiceBusReceiverOptions options) => new DeferredReceiver(this);
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class DeferredReceiver(DeferredClient client) : ServiceBusReceiver
    {
        public override Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveDeferredMessagesAsync(IEnumerable<long> sequenceNumbers,
            CancellationToken cancellationToken = default)
        {
            client.OnReceive?.Invoke();
            return Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(sequenceNumbers.Select(number => ServiceBusModelFactory.ServiceBusReceivedMessage(
                body: BinaryData.FromString("pending"), messageId: client.ReceivedMessageId, sequenceNumber: number,
                serviceBusMessageState: Azure.Messaging.ServiceBus.ServiceBusMessageState.Deferred)).ToArray());
        }

        public override Task AbandonMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null,
            CancellationToken cancellationToken = default)
        {
            client.Abandoned.Add(message.SequenceNumber);
            return client.AbandonFails ? Task.FromException(new ServiceBusException("abandon refused", ServiceBusFailureReason.ServiceCommunicationProblem)) : Task.CompletedTask;
        }

        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
        {
            client.Completed.Add(message.SequenceNumber);
            return client.CompleteFailure is { } failure ? Task.FromException(failure) : Task.CompletedTask;
        }

        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
