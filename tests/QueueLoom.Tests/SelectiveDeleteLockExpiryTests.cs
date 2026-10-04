using Azure.Messaging.ServiceBus;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

public sealed class SelectiveDeleteLockExpiryTests
{
    private static readonly ServiceBusEntityReference Orders = ServiceBusEntityReference.Queue("orders");

    [Fact]
    public async Task LockExpiryMidScanDoesNotReportAStillPresentMessageAsGone()
    {
        // 30 dead letters; the selected one (#25) sits in the third receive batch.
        var queue = new ExpiringLockQueue(Enumerable.Range(1, 30).Select(i => Message(i)));
        // The locks of the first batch expire while the scan is still running (PeekLock lock duration elapses),
        // so Service Bus hands those earlier messages out again before the unseen tail of the queue.
        queue.OnReceive = receive => { if (receive == 3) queue.ExpireLocks(1, 10); };
        var backups = new List<long>();

        var results = await SelectiveDeadLetterDeleter.DeleteAsync(
            queue,
            [new DeadLetterMessageKey(Orders, ServiceBusSubQueue.DeadLetter, 25, "message-25")],
            (message, _) => { lock (backups) backups.Add(message.SequenceNumber); return Task.CompletedTask; },
            batchSize: 10,
            maximumScanned: 1_000,
            emptyReceiveConfirmations: 2,
            TimeSpan.Zero,
            null,
            CancellationToken.None);

        var result = Assert.Single(results);
        // Before the fix the message stayed in the queue and was reported as "Not in the dead-letter queue any more".
        Assert.DoesNotContain(25L, queue.Remaining);
        Assert.Equal(DeadLetterMessageDeletionOutcome.Deleted, result.Outcome);
        Assert.Equal([25L], backups);
    }

    private static ServiceBusReceivedMessage Message(long sequenceNumber) =>
        ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString($"message-{sequenceNumber}"),
            messageId: $"message-{sequenceNumber}",
            sequenceNumber: sequenceNumber);

    /// <summary>PeekLock semantics: locked messages are skipped; locks can expire and messages come back in order.</summary>
    private sealed class ExpiringLockQueue(IEnumerable<ServiceBusReceivedMessage> messages) : IDeadLetterLockReceiver
    {
        private readonly List<ServiceBusReceivedMessage> _messages = [.. messages];
        private readonly HashSet<long> _locked = [];
        private int _receives;

        public Action<int>? OnReceive { get; set; }

        public List<long> Remaining { get { lock (_messages) return _messages.Select(m => m.SequenceNumber).ToList(); } }

        public void ExpireLocks(long from, long to)
        {
            lock (_messages) _locked.RemoveWhere(sequence => sequence >= from && sequence <= to);
        }

        public Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveAsync(int maxMessages, TimeSpan maxWaitTime, CancellationToken cancellationToken)
        {
            OnReceive?.Invoke(++_receives);
            lock (_messages)
            {
                var batch = _messages.Where(m => !_locked.Contains(m.SequenceNumber)).Take(maxMessages).ToArray();
                foreach (var message in batch) _locked.Add(message.SequenceNumber);
                return Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(batch);
            }
        }

        public Task CompleteAsync(ServiceBusReceivedMessage message)
        {
            lock (_messages)
            {
                _messages.RemoveAll(m => m.SequenceNumber == message.SequenceNumber);
                _locked.Remove(message.SequenceNumber);
            }
            return Task.CompletedTask;
        }

        public Task AbandonAsync(ServiceBusReceivedMessage message)
        {
            lock (_messages) _locked.Remove(message.SequenceNumber);
            return Task.CompletedTask;
        }
    }
}
