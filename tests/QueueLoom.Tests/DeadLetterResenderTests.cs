using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class DeadLetterResenderTests
{
    private static readonly ServiceBusEntityReference Orders = ServiceBusEntityReference.Queue("orders");

    [Fact]
    public async Task Copy_mode_sends_every_message_and_keeps_the_originals()
    {
        var workspace = new RecordingWorkspace();
        var items = new[] { Item(1), Item(2) };

        var result = await DeadLetterResender.ResendAsync(workspace, items, ResendMode.Copy);

        Assert.Equal(2, result.SentCount);
        Assert.All(result.Items, item => Assert.Equal(ResendOutcome.Sent, item.Outcome));
        Assert.Equal(["m-1", "m-2"], workspace.Sent.Select(request => request.Message.Properties.MessageId));
        Assert.Empty(workspace.DeleteRequests);
        Assert.Null(result.BackupDirectory);
    }

    [Fact]
    public async Task Move_mode_removes_only_the_originals_that_were_sent()
    {
        var workspace = new RecordingWorkspace { FailSendFor = "m-2" };
        var items = new[] { Item(1), Item(2), Item(3) };

        var result = await DeadLetterResender.ResendAsync(workspace, items, ResendMode.Move);

        var request = Assert.Single(workspace.DeleteRequests);
        Assert.Equal([1L, 3], request.Messages.Select(key => key.SequenceNumber));
        Assert.Equal([ResendOutcome.Moved, ResendOutcome.Failed, ResendOutcome.Moved], result.Items.Select(item => item.Outcome));
        Assert.Contains("broker said no", result.Items[1].Detail);
        Assert.Equal(workspace.BackupDirectory, result.BackupDirectory);
    }

    [Fact]
    public async Task A_copy_that_went_out_but_whose_original_was_not_found_is_reported()
    {
        var workspace = new RecordingWorkspace { NotFound = 2 };

        var result = await DeadLetterResender.ResendAsync(workspace, [Item(1), Item(2)], ResendMode.Move);

        Assert.Equal([ResendOutcome.Moved, ResendOutcome.SentOriginalKept], result.Items.Select(item => item.Outcome));
        Assert.Contains("no longer in the dead-letter queue", result.Items[1].Detail);
        Assert.Equal(1, result.OriginalsKeptCount);
    }

    [Fact]
    public async Task Cancelling_keeps_unsent_originals_but_still_removes_the_ones_already_sent()
    {
        using var cancellation = new CancellationTokenSource();
        var workspace = new RecordingWorkspace { AfterSend = () => cancellation.Cancel() };

        var result = await DeadLetterResender.ResendAsync(workspace, [Item(1), Item(2)], ResendMode.Move,
            cancellationToken: cancellation.Token);

        Assert.Equal([ResendOutcome.Moved, ResendOutcome.Cancelled], result.Items.Select(item => item.Outcome));
        Assert.Equal([1L], Assert.Single(workspace.DeleteRequests).Messages.Select(key => key.SequenceNumber));
    }

    [Fact]
    public async Task Active_messages_can_only_be_copied()
    {
        var active = new ResendItem(Message(1, ServiceBusSubQueue.Active), Orders, Message(1, ServiceBusSubQueue.Active).CreateDraft());

        await Assert.ThrowsAsync<ArgumentException>(() =>
            DeadLetterResender.ResendAsync(new RecordingWorkspace(), [active], ResendMode.Move));
    }

    [Fact]
    public void Subscriptions_go_back_to_their_topic()
    {
        Assert.Equal(ServiceBusEntityReference.Topic("events"),
            DeadLetterResender.OriginalDestination(ServiceBusEntityReference.Subscription("events", "billing")));
        Assert.Equal(Orders, DeadLetterResender.OriginalDestination(Orders));
    }

    private static ResendItem Item(long number)
    {
        var message = Message(number, ServiceBusSubQueue.DeadLetter);
        return new ResendItem(message, Orders, message.CreateDraft());
    }

    private static BrowsedMessage Message(long number, ServiceBusSubQueue subQueue) => new(
        Orders, subQueue, number, "{}"u8.ToArray(), new EditableMessageProperties(MessageId: $"m-{number}"));

    private sealed class RecordingWorkspace : IServiceBusWorkspace
    {
        public string BackupDirectory { get; } = Path.Combine(Path.GetTempPath(), "resend-backup");
        public string? FailSendFor { get; init; }
        public long? NotFound { get; init; }
        public Action? AfterSend { get; init; }
        public List<SendMessageRequest> Sent { get; } = [];
        public List<DeleteDeadLetterMessagesRequest> DeleteRequests { get; } = [];

        public WorkspaceConnectionState ConnectionState => WorkspaceConnectionState.Connected;
        public Guid? ConnectedProfileId { get; } = Guid.NewGuid();

        public Task SendMessageAsync(SendMessageRequest request, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (request.Message.Properties.MessageId == FailSendFor)
            {
                throw new InvalidOperationException("broker said no");
            }
            Sent.Add(request);
            AfterSend?.Invoke();
            return Task.CompletedTask;
        }

        public Task<DeleteDeadLetterMessagesResult> DeleteDeadLetterMessagesAsync(
            DeleteDeadLetterMessagesRequest request,
            CancellationToken cancellationToken = default,
            IProgress<DeadLetterMessageDeletionProgress>? progress = null)
        {
            DeleteRequests.Add(request);
            return Task.FromResult(new DeleteDeadLetterMessagesResult(
                ConnectedProfileId!.Value, DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch,
                request.Messages.Select(key => new DeadLetterMessageDeletionResult(key,
                    key.SequenceNumber == NotFound ? DeadLetterMessageDeletionOutcome.NotFound : DeadLetterMessageDeletionOutcome.Deleted)),
                BackupDirectory));
        }

        public Task ConnectAsync(ServiceBusProfile profile, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task DisconnectAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task SetAccessModeAsync(ProfileAccessMode accessMode, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<ServiceBusTopology> GetTopologyAsync(bool forceRefresh = false, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<IReadOnlyList<BrowsedMessage>> BrowseMessagesAsync(BrowseMessagesRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<DeadLetterSearchResult> SearchDeadLettersAsync(DeadLetterSearchRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task ResubmitDeadLetterAsync(ResubmitDeadLetterRequest request, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<DeadLetterPurgeResult> PurgeDeadLettersAsync(DeadLetterPurgeRequest request, CancellationToken cancellationToken = default, IProgress<DeadLetterPurgeProgress>? progress = null) => throw new NotSupportedException();
        public Task<DeadLetterSnapshot> GetDeadLetterSnapshotAsync(DeadLetterMonitorScope scope, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
