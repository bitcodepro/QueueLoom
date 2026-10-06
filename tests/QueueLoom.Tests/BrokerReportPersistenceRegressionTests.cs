using System.Reflection;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class BrokerReportPersistenceRegressionTests
{
    private static readonly ServiceBusEntityReference Orders = ServiceBusEntityReference.Queue("orders");

    [Theory]
    [InlineData("delete")]
    [InlineData("purge")]
    [InlineData("move")]
    [InlineData("durable-move")]
    public async Task BugCycleOne_ReportFailurePreservesConfirmedBrokerOutcomeAndBackup(string operation)
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        var receiver = new Receiver([Message(1)]);
        var report = operation == "purge" ? "purge-result.report" : "delete-result.report";
        string? session = null;
        receiver.OnComplete = () =>
        {
            var metadata = Assert.Single(Directory.GetFiles(paths.BackupsDirectory, "session.json", SearchOption.AllDirectories));
            session = Path.GetDirectoryName(metadata)!;
            Directory.CreateDirectory(Path.Combine(session, report));
        };
        await using var workspace = Workspace(paths, receiver);
        object? result = null;
        var error = await Record.ExceptionAsync(async () =>
        {
            if (operation == "purge")
            {
                var purge = await workspace.PurgeDeadLettersAsync(new DeadLetterPurgeRequest([Orders], [ServiceBusSubQueue.DeadLetter]));
                Assert.Equal(1, purge.DeletedCount);
                Assert.False(purge.HasFailures);
                Assert.Equal(session, purge.BackupDirectory);
                result = purge;
            }
            else if (operation == "delete")
            {
                var deletion = await workspace.DeleteDeadLetterMessagesAsync(new DeleteDeadLetterMessagesRequest([Key(1)]));
                Assert.Equal(DeadLetterMessageDeletionOutcome.Deleted, Assert.Single(deletion.Messages).Outcome);
                Assert.Equal(session, deletion.BackupDirectory);
                result = deletion;
            }
            else
            {
                var original = new BrowsedMessage(Orders, ServiceBusSubQueue.DeadLetter, 1, "body"u8.ToArray(), new EditableMessageProperties(MessageId: "m-1"));
                var item = new ResendItem(original, Orders, original.CreateDraft()).WithNewMessageId();
                ResendResult move;
                if (operation == "durable-move")
                {
                    var store = new BatchReplayStore(Path.Combine(directory.Path, "replays"));
                    var plan = await store.CreateResendAsync(workspace.ConnectedProfileId!.Value, [item], ResendMode.Move, 50,
                        workspace.ConnectedNamespace, workspace.ConnectedConfigurationIdentity!, "resend", default);
                    move = await store.RunItemsAsync(plan, [0], false, workspace, () => true, null, default);
                    Assert.Equal("Moved", Assert.Single(new BatchReplayStore(store.RootDirectory).ReadHistory(plan).Items).State);
                }
                else move = await DeadLetterResender.ResendAsync(workspace, [item], ResendMode.Move);
                Assert.Equal(ResendOutcome.Moved, Assert.Single(move.Items).Outcome);
                Assert.Equal(session, move.BackupDirectory);
                result = move;
            }
        });
        Assert.Equal([1L], receiver.Completed);
        Assert.Null(error);
        var json = JsonSerializer.Serialize(result);
        Assert.Contains(report, json, StringComparison.Ordinal);
        Assert.Contains("Warning", json, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(Directory.GetFiles(session!, "*.json", SearchOption.AllDirectories),
            path => Path.GetFileName(path) != "session.json");
    }

    [Fact]
    public async Task BugCycleOne_SelectedDeleteCleanupHasOneBudgetAndRetainsConfirmedResults()
    {
        using var directory = new TemporaryDirectory();
        using var cancellation = new CancellationTokenSource();
        var hung = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var receiver = new Receiver([Message(1), Message(2), Message(3)])
        {
            OnComplete = cancellation.Cancel,
            OnAbandon = (_, _) => hung.Task
        };
        await using var workspace = Workspace(QueueLoomPaths.ForRoot(directory.Path), receiver);
        AzureServiceBusWorkspace.AbandonBudgetOverride.Value = TimeSpan.FromMilliseconds(200);
        var task = workspace.DeleteDeadLetterMessagesAsync(new DeleteDeadLetterMessagesRequest([Key(1), Key(99)]), cancellation.Token);
        DeleteDeadLetterMessagesResult? result = null;
        Exception? error;
        try
        {
            error = await Record.ExceptionAsync(async () => result = await task.WaitAsync(TimeSpan.FromSeconds(3)));
        }
        finally
        {
            hung.TrySetResult();
            await task.WaitAsync(TimeSpan.FromSeconds(3));
            AzureServiceBusWorkspace.AbandonBudgetOverride.Value = null;
        }
        Assert.Null(error);
        Assert.Equal([1L], receiver.Completed);
        Assert.Equal([2L], receiver.Abandoned);
        Assert.True(receiver.LastAbandonToken.CanBeCanceled);
        Assert.Equal(DeadLetterMessageDeletionOutcome.Deleted, result!.Messages.Single(r => r.Message.SequenceNumber == 1).Outcome);
        Assert.Equal(DeadLetterMessageDeletionOutcome.Cancelled, result.Messages.Single(r => r.Message.SequenceNumber == 99).Outcome);
        Assert.Contains("lock", JsonSerializer.Serialize(result), StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Warning", JsonSerializer.Serialize(result), StringComparison.OrdinalIgnoreCase);
    }

    private static AzureServiceBusWorkspace Workspace(QueueLoomPaths paths, Receiver receiver)
    {
        var workspace = new AzureServiceBusWorkspace(new DeepAuditCloudTests.EmptyVault(), backupStore: new DeadLetterJsonBackupStore(paths));
        var profile = ServiceBusProfile.CreateNew("isolated", EnvironmentKind.Development, new(AuthenticationKind.ConnectionString)) with { AccessMode = ProfileAccessMode.ReadWrite };
        const BindingFlags flags = BindingFlags.Instance | BindingFlags.NonPublic;
        typeof(AzureServiceBusWorkspace).GetField("_client", flags)!.SetValue(workspace, new Client(receiver));
        typeof(AzureServiceBusWorkspace).GetField("_profile", flags)!.SetValue(workspace, profile);
        return workspace;
    }

    [Fact]
    public async Task BugCycleOne_ImmediateCleanupFailureStillReleasesLaterLocksAndWarns()
    {
        using var directory = new TemporaryDirectory();
        var receiver = new Receiver([Message(1), Message(2), Message(3)])
        {
            OnAbandon = (message, _) => message.SequenceNumber == 2
                ? Task.FromException(new IOException("release refused")) : Task.CompletedTask
        };
        await using var workspace = Workspace(QueueLoomPaths.ForRoot(directory.Path), receiver);
        var result = await workspace.DeleteDeadLetterMessagesAsync(new DeleteDeadLetterMessagesRequest([Key(1)]));
        Assert.Equal([2L, 3L], receiver.Abandoned);
        Assert.Equal(1, result.DeletedCount);
        Assert.Contains("1 held message lock", Assert.Single(result.Warnings), StringComparison.Ordinal);
    }

    private static DeadLetterMessageKey Key(long n) => new(Orders, ServiceBusSubQueue.DeadLetter, n, $"m-{n}");
    private static ServiceBusReceivedMessage Message(long n) => ServiceBusModelFactory.ServiceBusReceivedMessage(
        BinaryData.FromString("body"), messageId: $"m-{n}", sequenceNumber: n, lockTokenGuid: Guid.NewGuid());

    private sealed class Client(Receiver receiver) : ServiceBusClient
    {
        public override ServiceBusReceiver CreateReceiver(string queueName, ServiceBusReceiverOptions options) => receiver;
        public override ServiceBusSender CreateSender(string queueOrTopicName) => new Sender();
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class Sender : ServiceBusSender
    {
        public override ValueTask<ServiceBusMessageBatch> CreateMessageBatchAsync(CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(ServiceBusModelFactory.ServiceBusMessageBatch(0, [], new CreateMessageBatchOptions { MaxSizeInBytes = 262_144 }, tryAddCallback: _ => true));
        public override Task SendMessagesAsync(ServiceBusMessageBatch messageBatch, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class Receiver(IReadOnlyList<ServiceBusReceivedMessage> messages) : ServiceBusReceiver
    {
        private bool _received;
        public Action? OnComplete { get; set; }
        public Func<ServiceBusReceivedMessage, CancellationToken, Task>? OnAbandon { get; init; }
        public List<long> Completed { get; } = [];
        public List<long> Abandoned { get; } = [];
        public CancellationToken LastAbandonToken { get; private set; }
        public override Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveMessagesAsync(int maxMessages, TimeSpan? maxWaitTime = null, CancellationToken cancellationToken = default)
        {
            var batch = _received ? [] : messages;
            _received = true;
            return Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(batch);
        }
        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
        {
            Completed.Add(message.SequenceNumber);
            OnComplete?.Invoke();
            return Task.CompletedTask;
        }
        public override Task AbandonMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null, CancellationToken cancellationToken = default)
        {
            Abandoned.Add(message.SequenceNumber);
            LastAbandonToken = cancellationToken;
            return OnAbandon?.Invoke(message, cancellationToken) ?? Task.CompletedTask;
        }
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
