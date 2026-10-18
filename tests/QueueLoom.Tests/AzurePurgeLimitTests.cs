using System.Reflection;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// A purge that deleted exactly its safety limit stopped there and always said "Safety limit reached", even when the
// dead-letter queue was then empty: the operator was told messages remained when none did. At the limit it now peeks
// (without locking or removing anything) and reports the limit only when something is left.
public sealed partial class ViewModelStateTests
{
    [Theory]
    [InlineData(3, 3, false)]
    [InlineData(4, 3, true)]
    [InlineData(2, 3, false)]
    public async Task AzurePurge_ReportsTheSafetyLimitOnlyWhenMessagesRemain(int messages, int limit, bool limitReached)
    {
        using var directory = new TemporaryDirectory();
        var client = new CountedPurgeClient(messages);
        await using var azure = new AzureServiceBusWorkspace(new FakeSecretVault(),
            backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(directory.Path)));
        var profile = CreateProfile("Development", EnvironmentKind.Development, ProfileAccessMode.ReadWrite);
        typeof(AzureServiceBusWorkspace).GetField("_client", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure, client);
        typeof(AzureServiceBusWorkspace).GetField("_profile", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(azure, profile);

        var result = await azure.PurgeDeadLettersAsync(new DeadLetterPurgeRequest([ServiceBusEntityReference.Queue("orders")],
            [ServiceBusSubQueue.DeadLetter], maximumMessagesPerSubQueue: limit));

        var source = Assert.Single(result.Sources);
        Assert.Equal(Math.Min(messages, limit), source.DeletedCount);
        Assert.Equal(limitReached, source.LimitReached);
        if (!limitReached) Assert.DoesNotContain("Safety limit", source.Error ?? string.Empty, StringComparison.Ordinal);
    }

    private sealed class CountedPurgeClient(int messages) : ServiceBusClient
    {
        public Queue<long> Remaining { get; } = new(Enumerable.Range(1, messages).Select(number => (long)number));
        public override ServiceBusReceiver CreateReceiver(string queueName, ServiceBusReceiverOptions options) => new CountedPurgeReceiver(this);
        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class CountedPurgeReceiver(CountedPurgeClient client) : ServiceBusReceiver
    {
        public override Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveMessagesAsync(int maxMessages, TimeSpan? maxWaitTime = null,
            CancellationToken cancellationToken = default)
        {
            var batch = new List<ServiceBusReceivedMessage>();
            while (batch.Count < maxMessages && client.Remaining.TryDequeue(out var number))
            {
                batch.Add(ServiceBusModelFactory.ServiceBusReceivedMessage(body: BinaryData.FromString("dead"), messageId: $"m-{number}", sequenceNumber: number));
            }
            return Task.FromResult<IReadOnlyList<ServiceBusReceivedMessage>>(batch);
        }

        public override Task<ServiceBusReceivedMessage?> PeekMessageAsync(long? fromSequenceNumber = null, CancellationToken cancellationToken = default) =>
            Task.FromResult(client.Remaining.TryPeek(out var number)
                ? ServiceBusModelFactory.ServiceBusReceivedMessage(body: BinaryData.FromString("dead"), messageId: $"m-{number}", sequenceNumber: number)
                : null);

        public override Task CompleteMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken = default) => Task.CompletedTask;

        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
