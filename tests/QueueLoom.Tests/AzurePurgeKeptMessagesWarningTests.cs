using System.Reflection;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// When some completions of a backed-up purge batch failed, nothing said which messages were affected, although a failed
// completion does not say whether the broker deleted the message: their backups may be the only copies. Releasing
// messages was silent too, even when a release failed for a reason that may leave the message locked. Both are now
// warnings of the purge result (and its report); a lost lock holds nothing back and is not reported.
public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task AzurePurge_ATimedOutCompletionTheBrokerCarriedOutIsReportedAsUnconfirmedNotAsKept()
    {
        // Messages 1-3 are backed up. The completions of 2 and 3 time out in the SDK after the broker deleted them; their
        // release then finds no lock (MessageLockLost), which holds nothing back.
        var result = await PurgeWith(new KeptPurgeClient
        {
            CompletionFails = number => number == 1 ? null : new TaskCanceledException("The operation did not complete within the allocated time."),
            DeletedByTheBrokerAnyway = number => number != 1,
        });

        Assert.Equal(1, Assert.Single(result.Sources).DeletedCount);
        var warning = Assert.Single(result.Warnings);
        Assert.Contains("The deletion of 2 backed-up message(s) of orders was not confirmed (sequence numbers 2, 3)", warning, StringComparison.Ordinal);
        Assert.Contains("may still be in the dead-letter queue, or may have been deleted", warning, StringComparison.Ordinal);
        Assert.Contains("the backup is the only copy", warning, StringComparison.Ordinal);
        Assert.DoesNotContain("copies, not deletions", warning, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(ServiceBusFailureReason.ServiceCommunicationProblem, true)]
    [InlineData(ServiceBusFailureReason.MessageLockLost, false)]
    public async Task AzurePurge_OnlyAReleaseThatMayLeaveALockIsReported(ServiceBusFailureReason releaseFailure, bool reported)
    {
        var result = await PurgeWith(new KeptPurgeClient
        {
            CompletionFails = number => number == 1 ? null : new ServiceBusException("The service is busy.", ServiceBusFailureReason.ServiceBusy),
            ReleaseFails = new ServiceBusException("Release failed.", releaseFailure),
        });

        Assert.Equal(reported, result.Warnings.Any(warning =>
            warning.Contains("Release of 2 message lock(s) of orders was not confirmed; those messages may remain hidden", StringComparison.Ordinal)));
    }

    private async Task<DeadLetterPurgeResult> PurgeWith(KeptPurgeClient client)
    {
        using var directory = new TemporaryDirectory();
        await using var azure = new AzureServiceBusWorkspace(new FakeSecretVault(),
            backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(directory.Path)));
        typeof(AzureServiceBusWorkspace).GetField("_client", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure, client);
        typeof(AzureServiceBusWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure,
            CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite));
        return await azure.PurgeDeadLettersAsync(new DeadLetterPurgeRequest([ServiceBusEntityReference.Queue("orders")],
            [ServiceBusSubQueue.DeadLetter], maximumMessagesPerSubQueue: 10));
    }

    private sealed class KeptPurgeClient : ServiceBusClient
    {
        public Func<long, Exception?> CompletionFails { get; init; } = _ => null;
        public Func<long, bool> DeletedByTheBrokerAnyway { get; init; } = _ => false;
        public Exception? ReleaseFails { get; init; }
        public HashSet<long> Gone { get; } = [];
        public override ServiceBusReceiver CreateReceiver(string queueName, ServiceBusReceiverOptions options) => new KeptPurgeReceiver(this);
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class KeptPurgeReceiver(KeptPurgeClient client) : ServiceBusReceiver
    {
        private bool _received;

        public override Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveMessagesAsync(int maxMessages, TimeSpan? maxWaitTime = null,
            CancellationToken cancellationToken = default)
        {
            IReadOnlyList<ServiceBusReceivedMessage> batch = _received ? [] : new long[] { 1, 2, 3 }.Select(number =>
                ServiceBusModelFactory.ServiceBusReceivedMessage(body: BinaryData.FromString("dead"), messageId: $"m-{number}", sequenceNumber: number)).ToArray();
            _received = true;
            return Task.FromResult(batch);
        }

        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default)
        {
            if (client.DeletedByTheBrokerAnyway(message.SequenceNumber)) lock (client.Gone) client.Gone.Add(message.SequenceNumber);
            return client.CompletionFails(message.SequenceNumber) is { } failure ? Task.FromException(failure) : Task.CompletedTask;
        }

        public override Task AbandonMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null,
            CancellationToken cancellationToken = default)
        {
            lock (client.Gone)
            {
                if (client.Gone.Contains(message.SequenceNumber))
                    return Task.FromException(new ServiceBusException("The lock supplied is invalid.", ServiceBusFailureReason.MessageLockLost));
            }
            return client.ReleaseFails is { } failure ? Task.FromException(failure) : Task.CompletedTask;
        }

        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
