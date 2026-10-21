using System.Reflection;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// When a purge backed a batch up but could not complete some of its messages, those messages stayed in the dead-letter
// queue while their backup files looked like deletions, and nothing said which ones. Releasing them (or any batch) was
// silent too, even when the release failed and the messages stayed hidden until their locks expired. Both are now
// warnings of the purge result (and its report).
public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AzurePurge_SaysWhichBackedUpMessagesStayedAndWhetherTheyWereReleased(bool releaseFails)
    {
        using var directory = new TemporaryDirectory();
        var client = new KeptPurgeClient { ReleaseFails = releaseFails };
        await using var azure = new AzureServiceBusWorkspace(new FakeSecretVault(),
            backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(directory.Path)));
        typeof(AzureServiceBusWorkspace).GetField("_client", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure, client);
        typeof(AzureServiceBusWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure,
            CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite));

        // Messages 1-3 are backed up; the completions of 2 and 3 fail.
        var result = await azure.PurgeDeadLettersAsync(new DeadLetterPurgeRequest([ServiceBusEntityReference.Queue("orders")],
            [ServiceBusSubQueue.DeadLetter], maximumMessagesPerSubQueue: 10));

        Assert.Equal(1, Assert.Single(result.Sources).DeletedCount);
        Assert.Contains(result.Warnings, warning => warning.Contains("2 message(s) of orders were backed up but not deleted", StringComparison.Ordinal)
                                                    && warning.Contains("sequence numbers 2, 3", StringComparison.Ordinal));
        Assert.Equal(releaseFails, result.Warnings.Any(warning => warning.Contains("Release of 2 message lock(s) of orders was not confirmed", StringComparison.Ordinal)));
    }

    private sealed class KeptPurgeClient : ServiceBusClient
    {
        public bool ReleaseFails { get; init; }
        public override ServiceBusReceiver CreateReceiver(string queueName, ServiceBusReceiverOptions options) => new KeptPurgeReceiver(ReleaseFails);
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class KeptPurgeReceiver(bool releaseFails) : ServiceBusReceiver
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

        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default) =>
            message.SequenceNumber == 1
                ? Task.CompletedTask
                : Task.FromException(new ServiceBusException("The lock supplied is invalid.", ServiceBusFailureReason.MessageLockLost));

        public override Task AbandonMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null,
            CancellationToken cancellationToken = default) =>
            releaseFails ? Task.FromException(new ServiceBusException("The lock supplied is invalid.", ServiceBusFailureReason.MessageLockLost)) : Task.CompletedTask;

        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
