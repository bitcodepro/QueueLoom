using System.Reflection;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// A completion during a dead-letter purge that the SDK times out surfaces as a TaskCanceledException, although nobody
// cancelled. The settlement filter let it escape, so the whole purge threw: the result of every source already purged
// (and backed up) was lost, and the batch's other messages stayed locked until their locks expired. It is now that
// source's settlement error, like any other.
public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData("completion")]
    [InlineData("receive")]
    public async Task AzurePurge_AnSdkTimeoutIsThatSourcesErrorAndKeepsTheOtherResults(string timesOut)
    {
        using var directory = new TemporaryDirectory();
        var client = new TimeoutPurgeClient { ReceiveTimesOut = timesOut == "receive" };
        await using var azure = new AzureServiceBusWorkspace(new FakeSecretVault(),
            backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(directory.Path)));
        var profile = CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        typeof(AzureServiceBusWorkspace).GetField("_client", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure, client);
        typeof(AzureServiceBusWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure, profile);
        var first = ServiceBusEntityReference.Queue("first");
        var second = ServiceBusEntityReference.Queue("second");

        // "first" holds one message and is purged; in "second" the completion of message 2 times out.
        var result = await azure.PurgeDeadLettersAsync(new DeadLetterPurgeRequest([first, second], [ServiceBusSubQueue.DeadLetter],
            maximumMessagesPerSubQueue: 10));

        Assert.Equal(2, result.Sources.Count);
        Assert.Equal(1, result.Sources[0].DeletedCount);
        var timedOut = result.Sources[1];
        Assert.False(timedOut.IsSuccessful);
        Assert.NotNull(timedOut.Error);
        if (timesOut == "completion")
        {
            Assert.Equal(1, timedOut.DeletedCount);
            // The message whose completion timed out is released at once, not left locked.
            Assert.Equal([2L], client.Abandoned);
        }
        else
        {
            Assert.Equal(0, timedOut.DeletedCount);
        }
    }

    private sealed class TimeoutPurgeClient : ServiceBusClient
    {
        public List<long> Abandoned { get; } = [];
        public bool ReceiveTimesOut { get; init; }
        public override ServiceBusReceiver CreateReceiver(string queueName, ServiceBusReceiverOptions options) =>
            new TimeoutPurgeReceiver(this, queueName == "first" ? [10] : [1, 2]);
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class TimeoutPurgeReceiver(TimeoutPurgeClient client, long[] numbers) : ServiceBusReceiver
    {
        private bool _received;

        public override Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveMessagesAsync(int maxMessages, TimeSpan? maxWaitTime = null,
            CancellationToken cancellationToken = default)
        {
            if (client.ReceiveTimesOut && numbers.Length > 1)
            {
                return Task.FromException<IReadOnlyList<ServiceBusReceivedMessage>>(
                    new TaskCanceledException("The operation did not complete within the allocated time."));
            }
            // One batch, then the queue is empty; the purge stops on the settlement error before it asks again.
            IReadOnlyList<ServiceBusReceivedMessage> batch = _received ? [] : numbers.Select(number => ServiceBusModelFactory.ServiceBusReceivedMessage(
                body: BinaryData.FromString("dead"), messageId: $"m-{number}", sequenceNumber: number)).ToArray();
            _received = true;
            return Task.FromResult(batch);
        }

        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default) =>
            message.SequenceNumber == 2
                ? Task.FromException(new TaskCanceledException("The operation did not complete within the allocated time."))
                : Task.CompletedTask;

        public override Task AbandonMessageAsync(ServiceBusReceivedMessage message, IDictionary<string, object>? propertiesToModify = null,
            CancellationToken cancellationToken = default)
        {
            lock (client.Abandoned) client.Abandoned.Add(message.SequenceNumber);
            return Task.CompletedTask;
        }

        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
