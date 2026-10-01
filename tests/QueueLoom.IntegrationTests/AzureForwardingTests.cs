using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.IntegrationTests;

/// <summary>Auto-forwarding against the Service Bus emulator: the chain QueueLoom describes is the one messages take.</summary>
public sealed class AzureForwardingTests : IAsyncLifetime
{
    private readonly TemporaryDirectory _directory = new();
    private readonly string _topic = Emulators.Unique("fwd-topic");
    private readonly string _inbox = Emulators.Unique("fwd-inbox");
    private readonly string _archive = Emulators.Unique("fwd-archive");
    private readonly string _parked = Emulators.Unique("fwd-parked");
    private ServiceBusAdministrationClient _administration = null!;
    private ServiceBusClient _client = null!;
    private AzureServiceBusWorkspace _workspace = null!;

    public async ValueTask InitializeAsync()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(Emulators.ServiceBus)))
        {
            return;
        }

        var vault = new InMemorySecretVault();
        var profile = ServiceBusProfile.CreateNew(
            "Emulator", EnvironmentKind.Development, AuthenticationSettings.ConnectionString(), accessMode: ProfileAccessMode.ReadWrite);
        await vault.StoreAsync(ProfileSecretKey.ConnectionString(profile.Id), Emulators.ServiceBusConnectionString);
        _workspace = new AzureServiceBusWorkspace(vault, backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(_directory.Path)));
        await _workspace.ConnectAsync(profile);
        _administration = new ServiceBusAdministrationClient(
            EmulatorConnection.AdministrationConnectionString(Emulators.ServiceBusConnectionString, profile.EmulatorManagementPort));
        _client = new ServiceBusClient(Emulators.ServiceBusConnectionString);

        // topic → subscription "relay" → inbox → archive; parked receives inbox's dead letters.
        await _administration.CreateQueueAsync(_archive);
        await _administration.CreateQueueAsync(_parked);
        await _administration.CreateQueueAsync(new CreateQueueOptions(_inbox) { ForwardTo = _archive, ForwardDeadLetteredMessagesTo = _parked });
        await _administration.CreateTopicAsync(_topic);
        await _administration.CreateSubscriptionAsync(new CreateSubscriptionOptions(_topic, "relay") { ForwardTo = _inbox });
        await _administration.CreateSubscriptionAsync(new CreateSubscriptionOptions(_topic, "keep"));
    }

    public async ValueTask DisposeAsync()
    {
        if (_administration is not null)
        {
            await _administration.DeleteTopicAsync(_topic);
            foreach (var queue in new[] { _inbox, _archive, _parked })
            {
                await _administration.DeleteQueueAsync(queue);
            }
            await _client.DisposeAsync();
        }
        if (_workspace is not null)
        {
            await _workspace.DisposeAsync();
        }
        _directory.Dispose();
    }

    [EmulatorFact(Emulators.ServiceBus)]
    public async Task The_chain_QueueLoom_shows_is_where_messages_end_up()
    {
        var topology = await _workspace.GetTopologyAsync(forceRefresh: true);
        var inbox = topology.Queues.Single(queue => queue.Name == _inbox);
        Assert.Equal(_archive, Forwarding.TargetName(inbox.ForwardTo), StringComparer.OrdinalIgnoreCase);
        Assert.Contains($"Forwards to {_archive}", inbox.Note, StringComparison.OrdinalIgnoreCase);
        Assert.Contains($"Dead letters are forwarded to {_parked}", inbox.Note, StringComparison.OrdinalIgnoreCase);
        var archive = topology.Queues.Single(queue => queue.Name == _archive);
        Assert.Contains("Gets forwarded messages from", archive.Note, StringComparison.Ordinal);
        var relay = topology.Topics.Single(topic => topic.Name == _topic).Subscriptions.Single(item => item.Name == "relay");
        Assert.Contains($"Forwards to {_inbox} → {_archive}", relay.Note, StringComparison.OrdinalIgnoreCase);

        var report = Forwarding.Follow(topology, _topic);
        Assert.Equal(new[] { _archive, $"{_topic}/keep" }.Order(StringComparer.Ordinal),
            report.Destinations.Order(StringComparer.Ordinal), StringComparer.OrdinalIgnoreCase);
        Assert.Equal(2, report.LongestChain);

        var rules = await _workspace.GetTopicRulesAsync(_topic);
        Assert.Contains($"Forwards to {_inbox} → {_archive}", rules.Single(item => item.Subscription == "relay").Note, StringComparison.OrdinalIgnoreCase);

        var browse = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _workspace.BrowseMessagesAsync(new BrowseMessagesRequest(ServiceBusEntityReference.Queue(_inbox), ServiceBusSubQueue.Active, 10)));
        Assert.Contains($"Look in {_archive}", browse.Message, StringComparison.OrdinalIgnoreCase);

        await using var sender = _client.CreateSender(_topic);
        await sender.SendMessageAsync(new ServiceBusMessage("hello") { MessageId = "fwd-1" });
        await using var receiver = _client.CreateReceiver(_archive);
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(received);
        Assert.Equal("fwd-1", received.MessageId);
    }
}
