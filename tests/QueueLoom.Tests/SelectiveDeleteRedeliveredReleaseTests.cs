using Azure.Messaging.ServiceBus;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

// When a lock expired mid-scan, Service Bus hands the message out again with a new lock, and the scan held it a second
// time next to its expired first copy. Releasing that stale copy always fails (its lock is gone), so every redelivered
// message counted as an unconfirmed release and the warning overstated how many locks were left behind. A message is
// now held once, by its latest delivery.
public sealed class SelectiveDeleteRedeliveredReleaseTests
{
    private static readonly ServiceBusEntityReference Orders = ServiceBusEntityReference.Queue("orders");

    [Fact]
    public async Task ARedeliveredMessageIsHeldOnceAndReleasedWithItsCurrentLock()
    {
        var queue = new RedeliveringQueue(30);
        queue.OnReceive = receive => { if (receive == 3) queue.ExpireLocks(1, 10); };
        var warnings = new List<string>();

        var results = await SelectiveDeadLetterDeleter.DeleteAsync(queue,
            [new DeadLetterMessageKey(Orders, ServiceBusSubQueue.DeadLetter, 25, "message-25")],
            (_, _) => Task.CompletedTask,
            batchSize: 10, maximumScanned: 1_000, emptyReceiveConfirmations: 2, TimeSpan.Zero, null, CancellationToken.None,
            warnings.Add);

        Assert.Equal(DeadLetterMessageDeletionOutcome.Deleted, Assert.Single(results).Outcome);
        Assert.Empty(warnings);
        Assert.Equal(29, queue.ReleasedWithCurrentLock);
        Assert.Equal(0, queue.ReleasesWithAStaleLock);
    }

    /// <summary>
    /// PeekLock as Service Bus does it: every delivery is a new message instance with its own lock, and only the latest
    /// delivery's lock can be released; releasing an earlier one fails with MessageLockLost.
    /// </summary>
    private sealed class RedeliveringQueue(int count) : IDeadLetterLockReceiver
    {
        private readonly List<long> _messages = [.. Enumerable.Range(1, count).Select(i => (long)i)];
        private readonly HashSet<long> _locked = [];
        private readonly Dictionary<long, ServiceBusReceivedMessage> _current = [];
        private int _receives;

        public Action<int>? OnReceive { get; set; }
        public int ReleasedWithCurrentLock { get; private set; }
        public int ReleasesWithAStaleLock { get; private set; }

        public void ExpireLocks(long from, long to)
        {
            lock (_messages) _locked.RemoveWhere(sequence => sequence >= from && sequence <= to);
        }

        public Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveAsync(int maxMessages, TimeSpan maxWaitTime, CancellationToken cancellationToken)
        {
            OnReceive?.Invoke(++_receives);
            lock (_messages)
            {
                var batch = _messages.Where(sequence => !_locked.Contains(sequence)).Take(maxMessages).Select(sequence =>
                {
                    _locked.Add(sequence);
                    return _current[sequence] = ServiceBusModelFactory.ServiceBusReceivedMessage(
                        body: BinaryData.FromString($"message-{sequence}"), messageId: $"message-{sequence}", sequenceNumber: sequence,
                        lockTokenGuid: Guid.NewGuid());
                }).ToArray();
                return Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(batch);
            }
        }

        public Task CompleteAsync(ServiceBusReceivedMessage message)
        {
            lock (_messages)
            {
                _messages.Remove(message.SequenceNumber);
                _locked.Remove(message.SequenceNumber);
            }
            return Task.CompletedTask;
        }

        public Task AbandonAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken)
        {
            lock (_messages)
            {
                if (!ReferenceEquals(_current[message.SequenceNumber], message) || !_locked.Contains(message.SequenceNumber))
                {
                    ReleasesWithAStaleLock++;
                    return Task.FromException(new ServiceBusException("The lock supplied is invalid.", ServiceBusFailureReason.MessageLockLost));
                }
                _locked.Remove(message.SequenceNumber);
                ReleasedWithCurrentLock++;
                return Task.CompletedTask;
            }
        }
    }
}
