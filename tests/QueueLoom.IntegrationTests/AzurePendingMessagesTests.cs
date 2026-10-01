using Azure.Messaging.ServiceBus;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;
using AzureMessage = Azure.Messaging.ServiceBus.ServiceBusMessage;
using AzureMessageState = Azure.Messaging.ServiceBus.ServiceBusMessageState;
using DomainMessageState = QueueLoom.Core.ServiceBus.ServiceBusMessageState;

namespace QueueLoom.IntegrationTests;

/// <summary>Scheduled and deferred messages in the "orders" queue of the Azure Service Bus emulator.</summary>
public sealed class AzurePendingMessagesTests : IAsyncLifetime
{
    private const string Queue = "orders";
    private readonly TemporaryDirectory _directory = new();
    private ServiceBusClient _client = null!;
    private AzureServiceBusWorkspace _workspace = null!;

    public async ValueTask InitializeAsync()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(Emulators.ServiceBus)))
        {
            return;
        }

        _client = new ServiceBusClient(Emulators.ServiceBusConnectionString);
        await DrainAsync();
        var vault = new InMemorySecretVault();
        var profile = ServiceBusProfile.CreateNew(
            "Emulator", EnvironmentKind.Development, AuthenticationSettings.ConnectionString(), accessMode: ProfileAccessMode.ReadWrite);
        await vault.StoreAsync(ProfileSecretKey.ConnectionString(profile.Id), Emulators.ServiceBusConnectionString);
        _workspace = new AzureServiceBusWorkspace(vault, backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(_directory.Path)));
        await _workspace.ConnectAsync(profile);
        await _workspace.GetTopologyAsync(forceRefresh: true);
    }

    public async ValueTask DisposeAsync()
    {
        if (_workspace is not null)
        {
            await _workspace.DisposeAsync();
        }
        if (_client is not null)
        {
            await DrainAsync();
            await _client.DisposeAsync();
        }
        _directory.Dispose();
    }

    [EmulatorFact(Emulators.ServiceBus)]
    public async Task Scheduled_and_deferred_messages_are_listed_backed_up_and_removed()
    {
        var run = Guid.NewGuid().ToString("N")[..8];
        string Id(string name) => $"{name}-{run}";
        await using var sender = _client.CreateSender(Queue);
        var due = DateTimeOffset.UtcNow.AddHours(2);
        await sender.ScheduleMessageAsync(new AzureMessage("pay later") { MessageId = Id("scheduled-1") }, due);
        await sender.SendMessageAsync(new AzureMessage("check stock") { MessageId = Id("deferred-1") });
        await sender.SendMessageAsync(new AzureMessage("normal") { MessageId = Id("active-1") });
        await using (var receiver = _client.CreateReceiver(Queue))
        {
            var received = await ReceiveAsync(receiver, Id("deferred-1"));
            await receiver.DeferMessageAsync(received);
        }

        var queue = ServiceBusEntityReference.Queue(Queue);
        var listed = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(queue));
        var scheduled = listed.Single(message => message.Properties.MessageId == Id("scheduled-1"));
        var deferred = listed.Single(message => message.Properties.MessageId == Id("deferred-1"));
        Assert.Equal(DomainMessageState.Scheduled, scheduled.State);
        Assert.Equal(due.ToUnixTimeSeconds(), scheduled.Properties.ScheduledEnqueueTime!.Value.ToUnixTimeSeconds());
        Assert.Equal(DomainMessageState.Deferred, deferred.State);
        Assert.False(PendingMessages.IsPending(listed.Single(message => message.Properties.MessageId == Id("active-1"))));

        var result = await _workspace.RemovePendingMessagesAsync([scheduled, deferred]);

        Assert.True(result.RemovedCount == 2,
            string.Join(" | ", result.Messages.Select(item => $"{item.Message.Properties.MessageId}: {item.Outcome} {item.Detail}")));
        Assert.Equal(2, Directory.EnumerateFiles(result.BackupDirectory, "*.json", SearchOption.AllDirectories)
            .Count(file => Path.GetFileName(Path.GetDirectoryName(file)) == "active"));
        var after = await _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(queue));
        Assert.Equal([Id("active-1")], after.Select(message => message.Properties.MessageId).Where(id => id!.EndsWith(run, StringComparison.Ordinal)));

        var again = await _workspace.RemovePendingMessagesAsync([scheduled, deferred]);
        Assert.Equal(2, again.NotFoundCount);
    }

    private static async Task<ServiceBusReceivedMessage> ReceiveAsync(ServiceBusReceiver receiver, string messageId)
    {
        for (var attempt = 0; attempt < 10; attempt++)
        {
            foreach (var message in await receiver.ReceiveMessagesAsync(10, TimeSpan.FromSeconds(2)))
            {
                if (message.MessageId == messageId)
                {
                    return message;
                }
                await receiver.AbandonMessageAsync(message);
            }
        }
        throw new InvalidOperationException($"Message {messageId} was not received.");
    }

    private async Task DrainAsync()
    {
        await using var receiver = _client.CreateReceiver(Queue, new ServiceBusReceiverOptions { ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete });
        while ((await receiver.ReceiveMessagesAsync(100, TimeSpan.FromSeconds(1))).Count > 0)
        {
        }

        await using var sender = _client.CreateSender(Queue);
        await using var locker = _client.CreateReceiver(Queue);
        foreach (var message in await receiver.PeekMessagesAsync(250))
        {
            if (message.State == AzureMessageState.Scheduled)
            {
                await sender.CancelScheduledMessageAsync(message.SequenceNumber);
            }
            else if (message.State == AzureMessageState.Deferred)
            {
                try
                {
                    foreach (var deferred in await locker.ReceiveDeferredMessagesAsync([message.SequenceNumber]))
                    {
                        await locker.CompleteMessageAsync(deferred);
                    }
                }
                catch (ServiceBusException exception) when (exception.Reason == ServiceBusFailureReason.MessageNotFound)
                {
                    // Still locked by an earlier run; it is removed once the lock expires.
                }
            }
        }
    }
}
