using Azure.Messaging.ServiceBus;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

public sealed class SelectiveDeadLetterDeleterTests
{
    private static readonly ServiceBusEntityReference Orders = ServiceBusEntityReference.Queue("orders");

    [Fact]
    public async Task CycleOne_SettlementTimeoutPreservesConfirmedSiblingAndDoesNotClaimUnknownDeletion()
    {
        var queue = new FakeLockQueue([Message(1), Message(2)]);
        queue.OnComplete = message => { if (message.SequenceNumber == 2) throw new TaskCanceledException("settlement timeout"); };
        var backups = new List<long>();
        IReadOnlyList<DeadLetterMessageDeletionResult>? results = null;
        var error = await Record.ExceptionAsync(async () => results = await DeleteAsync(queue, [Key(1), Key(2)], backups));
        Assert.Equal([1L, 2L], backups.Order());
        Assert.Equal([2L], queue.Remaining);
        Assert.Null(error);
        Assert.Equal(DeadLetterMessageDeletionOutcome.Deleted, results!.Single(r => r.Message.SequenceNumber == 1).Outcome);
        var unknown = results!.Single(r => r.Message.SequenceNumber == 2);
        Assert.Equal(DeadLetterMessageDeletionOutcome.Failed, unknown.Outcome);
        Assert.Contains("may still be", unknown.Detail, StringComparison.Ordinal);
        Assert.Contains("settlement timeout", unknown.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CycleOne_CleanupTimeoutPreservesConfirmedDeletionAndContinuesReleasingOtherLocks()
    {
        var queue = new FakeLockQueue([Message(1), Message(2), Message(3)]);
        var releases = new List<long>();
        queue.OnAbandon = message => { releases.Add(message.SequenceNumber); if (message.SequenceNumber == 2) throw new TaskCanceledException("cleanup timeout"); };
        IReadOnlyList<DeadLetterMessageDeletionResult>? results = null;
        var error = await Record.ExceptionAsync(async () => results = await DeleteAsync(queue, [Key(1)], []));
        Assert.Equal([2L, 3L], queue.Remaining);
        Assert.Null(error);
        Assert.Equal(DeadLetterMessageDeletionOutcome.Deleted, Assert.Single(results!).Outcome);
        Assert.Equal([2L, 3L], releases);
        Assert.Equal([2L], queue.Locked);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CycleOne_ReceiveFailurePreservesAcknowledgedDeletionAndReleasesOtherLocks(bool timeout)
    {
        var queue = new FakeLockQueue([Message(1), Message(2), Message(3)]);
        queue.OnReceive = count => { if (count == 2) { if (timeout) throw new TaskCanceledException("receive timeout"); throw new IOException("receive failed"); } };
        var backups = new List<long>();
        IReadOnlyList<DeadLetterMessageDeletionResult>? results = null;
        var error = await Record.ExceptionAsync(async () => results = await DeleteAsync(queue, [Key(1), Key(3)], backups, batchSize: 2));
        Assert.Equal([1L], backups);
        Assert.Equal([2L, 3L], queue.Remaining);
        Assert.Empty(queue.Locked);
        Assert.Null(error);
        Assert.Equal(DeadLetterMessageDeletionOutcome.Deleted, results!.Single(r => r.Message.SequenceNumber == 1).Outcome);
        var unfinished = results!.Single(r => r.Message.SequenceNumber == 3);
        Assert.Equal(DeadLetterMessageDeletionOutcome.Failed, unfinished.Outcome);
        Assert.Contains(timeout ? "receive timeout" : "receive failed", unfinished.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DeletesOnlyTheSelectedMessagesAndReleasesTheRest()
    {
        var queue = new FakeLockQueue(Enumerable.Range(1, 10).Select(i => Message(i)));
        var backups = new List<long>();

        var results = await DeleteAsync(queue, [Key(3), Key(7)], backups);

        Assert.All(results, result => Assert.Equal(DeadLetterMessageDeletionOutcome.Deleted, result.Outcome));
        Assert.Equal([3L, 7L], backups.Order());
        Assert.Equal([1L, 2, 4, 5, 6, 8, 9, 10], queue.Remaining);
        Assert.Empty(queue.Locked);
    }

    [Fact]
    public async Task StopsReceivingOnceEverySelectedMessageIsDeleted()
    {
        var queue = new FakeLockQueue(Enumerable.Range(1, 500).Select(i => Message(i)));

        await DeleteAsync(queue, [Key(2)], [], batchSize: 10);

        Assert.Equal(10, queue.ReceivedCount);
        Assert.Empty(queue.Locked);
    }

    [Fact]
    public async Task DifferentMessageIdUnderTheSameSequenceNumber_IsLeftAlone()
    {
        var queue = new FakeLockQueue([Message(5, messageId: "replaced")]);

        var result = Assert.Single(await DeleteAsync(queue, [Key(5, "original")], []));

        Assert.Equal(DeadLetterMessageDeletionOutcome.NotFound, result.Outcome);
        Assert.Contains("different message", result.Detail, StringComparison.Ordinal);
        Assert.Equal([5L], queue.Remaining);
    }

    [Fact]
    public async Task MessageThatIsGone_IsReportedAsNotFound()
    {
        var queue = new FakeLockQueue([Message(1), Message(2)]);

        var result = Assert.Single(await DeleteAsync(queue, [Key(99)], []));

        Assert.Equal(DeadLetterMessageDeletionOutcome.NotFound, result.Outcome);
        Assert.Contains("Not in the dead-letter queue", result.Detail, StringComparison.Ordinal);
        Assert.Equal([1L, 2], queue.Remaining);
    }

    [Fact]
    public async Task ScanLimit_StopsTheSearchAndSaysSo()
    {
        var queue = new FakeLockQueue(Enumerable.Range(1, 50).Select(i => Message(i)));

        var result = Assert.Single(await DeleteAsync(queue, [Key(40)], [], maximumScanned: 20));

        Assert.Equal(DeadLetterMessageDeletionOutcome.NotFound, result.Outcome);
        Assert.Contains("first 20", result.Detail, StringComparison.Ordinal);
        Assert.Equal(50, queue.Remaining.Count);
    }

    [Fact]
    public async Task FailedBackup_KeepsTheMessage()
    {
        var queue = new FakeLockQueue([Message(1), Message(2)]);

        var results = await SelectiveDeadLetterDeleter.DeleteAsync(
            queue,
            [Key(2)],
            (_, _) => throw new IOException("disk full"),
            batchSize: 10,
            maximumScanned: 100,
            emptyReceiveConfirmations: 2,
            TimeSpan.Zero,
            null,
            CancellationToken.None);

        var result = Assert.Single(results);
        Assert.Equal(DeadLetterMessageDeletionOutcome.Failed, result.Outcome);
        Assert.Contains("disk full", result.Detail, StringComparison.Ordinal);
        Assert.Equal([1L, 2], queue.Remaining);
        Assert.Empty(queue.Locked);
    }

    [Fact]
    public async Task FailedSettlement_IsReportedAsFailed()
    {
        var queue = new FakeLockQueue([Message(1)]) { FailComplete = true };

        var result = Assert.Single(await DeleteAsync(queue, [Key(1)], []));

        Assert.Equal(DeadLetterMessageDeletionOutcome.Failed, result.Outcome);
        Assert.Contains("Backed up", result.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Cancellation_ReportsUnprocessedMessagesAndReleasesLocks()
    {
        var queue = new FakeLockQueue(Enumerable.Range(1, 30).Select(i => Message(i)));
        using var cancellation = new CancellationTokenSource();
        queue.OnReceive = count => { if (count == 2) cancellation.Cancel(); };

        var results = await SelectiveDeadLetterDeleter.DeleteAsync(
            queue,
            [Key(3), Key(25)],
            (_, _) => Task.CompletedTask,
            batchSize: 5,
            maximumScanned: 100,
            emptyReceiveConfirmations: 2,
            TimeSpan.Zero,
            null,
            cancellation.Token);

        Assert.Equal(DeadLetterMessageDeletionOutcome.Deleted, results.Single(result => result.Message.SequenceNumber == 3).Outcome);
        Assert.Equal(DeadLetterMessageDeletionOutcome.Cancelled, results.Single(result => result.Message.SequenceNumber == 25).Outcome);
        Assert.Contains(25L, queue.Remaining);
        Assert.Empty(queue.Locked);
    }

    [Fact]
    public async Task ExpiredLocks_DoNotLoopForever()
    {
        var queue = new FakeLockQueue(Enumerable.Range(1, 5).Select(i => Message(i))) { LocksExpireImmediately = true };

        var result = Assert.Single(await DeleteAsync(queue, [Key(99)], [], batchSize: 5));

        Assert.Equal(DeadLetterMessageDeletionOutcome.NotFound, result.Outcome);
        Assert.True(queue.ReceivedCount <= 10);
        Assert.Equal(5, queue.Remaining.Count);
    }

    [Fact]
    public void Request_RejectsActiveMessagesAndOversizedSelections()
    {
        Assert.Throws<ArgumentException>(() => new DeleteDeadLetterMessagesRequest(
            [new DeadLetterMessageKey(Orders, ServiceBusSubQueue.Active, 1)]));
        Assert.Throws<ArgumentException>(() => new DeleteDeadLetterMessagesRequest(
            Enumerable.Range(1, DeleteDeadLetterMessagesRequest.MaximumMessages + 1).Select(i => Key(i))));
        Assert.Throws<ArgumentException>(() => new DeleteDeadLetterMessagesRequest([]));

        var request = new DeleteDeadLetterMessagesRequest([Key(1), Key(1), Key(2)]);
        Assert.Equal(2, request.Messages.Count);
    }

    private static Task<IReadOnlyList<DeadLetterMessageDeletionResult>> DeleteAsync(
        FakeLockQueue queue,
        IReadOnlyCollection<DeadLetterMessageKey> selection,
        List<long> backups,
        int batchSize = 10,
        int maximumScanned = 1_000) =>
        SelectiveDeadLetterDeleter.DeleteAsync(
            queue,
            selection,
            (message, _) =>
            {
                lock (backups)
                {
                    backups.Add(message.SequenceNumber);
                }
                return Task.CompletedTask;
            },
            batchSize,
            maximumScanned,
            emptyReceiveConfirmations: 2,
            TimeSpan.Zero,
            null,
            CancellationToken.None);

    private static DeadLetterMessageKey Key(long sequenceNumber, string? messageId = null) =>
        new(Orders, ServiceBusSubQueue.DeadLetter, sequenceNumber, messageId ?? $"message-{sequenceNumber}");

    private static ServiceBusReceivedMessage Message(long sequenceNumber, string? messageId = null) =>
        ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString($"message-{sequenceNumber}"),
            messageId: messageId ?? $"message-{sequenceNumber}",
            sequenceNumber: sequenceNumber);

    /// <summary>A dead-letter queue with PeekLock semantics: locked messages are skipped until completed or abandoned.</summary>
    private sealed class FakeLockQueue(IEnumerable<ServiceBusReceivedMessage> messages) : IDeadLetterLockReceiver
    {
        private readonly List<ServiceBusReceivedMessage> _messages = [.. messages];
        private readonly HashSet<long> _locked = [];
        private int _receives;

        public bool FailComplete { get; init; }

        public bool LocksExpireImmediately { get; init; }

        public Action<int>? OnReceive { get; set; }

        public Action<ServiceBusReceivedMessage>? OnComplete { get; set; }

        public Action<ServiceBusReceivedMessage>? OnAbandon { get; set; }

        public int ReceivedCount { get; private set; }

        public List<long> Remaining => _messages.Select(message => message.SequenceNumber).ToList();

        public IReadOnlyCollection<long> Locked => _locked;

        public Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveAsync(
            int maxMessages,
            TimeSpan maxWaitTime,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            OnReceive?.Invoke(++_receives);
            if (LocksExpireImmediately)
            {
                _locked.Clear();
            }
            var batch = _messages
                .Where(message => !_locked.Contains(message.SequenceNumber))
                .Take(maxMessages)
                .ToArray();
            foreach (var message in batch)
            {
                _locked.Add(message.SequenceNumber);
            }
            ReceivedCount += batch.Length;
            return Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(batch);
        }

        public Task CompleteAsync(ServiceBusReceivedMessage message)
        {
            OnComplete?.Invoke(message);
            if (FailComplete)
            {
                throw new InvalidOperationException("lock lost");
            }
            lock (_messages)
            {
                _messages.RemoveAll(item => item.SequenceNumber == message.SequenceNumber);
                _locked.Remove(message.SequenceNumber);
            }
            return Task.CompletedTask;
        }

        public Task AbandonAsync(ServiceBusReceivedMessage message)
        {
            OnAbandon?.Invoke(message);
            lock (_messages)
            {
                _locked.Remove(message.SequenceNumber);
            }
            return Task.CompletedTask;
        }
    }
}
