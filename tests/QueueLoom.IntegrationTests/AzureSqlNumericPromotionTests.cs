using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.IntegrationTests;

/// <summary>
/// Finite Int64/Double SQL filters against the Service Bus emulator: the preview and the messages
/// a subscription actually receives. A non-finite literal such as 1e400 is not part of this check.
/// </summary>
public sealed class AzureSqlNumericPromotionTests : IAsyncLifetime
{
    private readonly TemporaryDirectory _directory = new();
    private readonly string _topic = Emulators.Unique("numeric");
    private ServiceBusAdministrationClient _administration = null!;
    private ServiceBusClient _client = null!;
    private AzureServiceBusWorkspace _workspace = null!;

    private static readonly (string Name, string Filter)[] Subscriptions =
    [
        ("eq-double", "id = 9007199254740992.0"),
        ("gt-double", "id > 9007199254740992.0"),
        ("eq-long", "id = 9007199254740992"),
        ("gt-long", "id > 9007199254740992"),
        ("times", "id * 0.1 = 0.3")
    ];

    public async ValueTask InitializeAsync()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(Emulators.ServiceBus)))
        {
            return;
        }

        var vault = new InMemorySecretVault();
        var profile = ServiceBusProfile.CreateNew(
                "Emulator", EnvironmentKind.Development, AuthenticationSettings.ConnectionString(), accessMode: ProfileAccessMode.ReadWrite)
            with { AllowQueueManagement = true };
        await vault.StoreAsync(ProfileSecretKey.ConnectionString(profile.Id), Emulators.ServiceBusConnectionString);
        _workspace = new AzureServiceBusWorkspace(vault, backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(_directory.Path)));
        await _workspace.ConnectAsync(profile);

        _administration = new ServiceBusAdministrationClient(
            EmulatorConnection.AdministrationConnectionString(Emulators.ServiceBusConnectionString, profile.EmulatorManagementPort));
        _client = new ServiceBusClient(Emulators.ServiceBusConnectionString);
        await _administration.CreateTopicAsync(new CreateTopicOptions(_topic) { DefaultMessageTimeToLive = TimeSpan.FromMinutes(30) });
        foreach (var (name, filter) in Subscriptions)
        {
            await _administration.CreateSubscriptionAsync(
                new CreateSubscriptionOptions(_topic, name) { DefaultMessageTimeToLive = TimeSpan.FromMinutes(30) },
                new CreateRuleOptions(name, new SqlRuleFilter(filter)));
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_administration is not null)
        {
            await _administration.DeleteTopicAsync(_topic);
            await _client.DisposeAsync();
        }
        if (_workspace is not null)
        {
            await _workspace.DisposeAsync();
        }
        _directory.Dispose();
    }

    [EmulatorFact(Emulators.ServiceBus)]
    public async Task Preview_matches_Service_Bus_for_long_and_double_promotion()
    {
        var rules = await _workspace.GetTopicRulesAsync(_topic);
        (string Id, object Value)[] messages =
        [
            ("wide", 9007199254740993L),
            ("small", 3L),
            ("precise", 9007199254740992d)
        ];

        var sender = _client.CreateSender(_topic);
        foreach (var (id, value) in messages)
        {
            var message = new ServiceBusMessage("{}") { MessageId = id };
            message.ApplicationProperties["id"] = value;
            await sender.SendMessageAsync(message);
        }

        var delivered = new Dictionary<string, HashSet<string>>();
        foreach (var (name, _) in Subscriptions)
        {
            await using var receiver = _client.CreateReceiver(_topic, name);
            var ids = new HashSet<string>();
            for (var attempt = 0; attempt < 10; attempt++)
            {
                foreach (var message in await receiver.PeekMessagesAsync(20))
                {
                    ids.Add(message.MessageId);
                }
                await Task.Delay(200);
            }
            delivered[name] = ids;
        }

        foreach (var (id, value) in messages)
        {
            var routing = TopicRouting.Route(_topic, rules, new RoutingMessage(
                new EditableMessageProperties(MessageId: id),
                new Dictionary<string, object?> { ["id"] = value }));
            foreach (var subscription in routing.Subscriptions)
            {
                var actual = delivered[subscription.Subscription].Contains(id);
                Assert.True(subscription.Outcome != RoutingOutcome.Unknown,
                    $"{id} → {subscription.Subscription}: {subscription.Summary}");
                Assert.True(actual == (subscription.Outcome == RoutingOutcome.Receives),
                    $"{id} ({value.GetType().Name} {value}) → {subscription.Subscription}: " +
                    $"Service Bus {(actual ? "delivered" : "did not deliver")}, QueueLoom said {subscription.Summary}");
            }
        }

        // The documented answers, so a preview that merely agrees with a wrong delivery still fails.
        AssertOutcome(rules, "wide", 9007199254740993L, "eq-double", RoutingOutcome.Receives);
        AssertOutcome(rules, "wide", 9007199254740993L, "gt-double", RoutingOutcome.Skips);
        AssertOutcome(rules, "wide", 9007199254740993L, "eq-long", RoutingOutcome.Skips);
        AssertOutcome(rules, "wide", 9007199254740993L, "gt-long", RoutingOutcome.Receives);
        AssertOutcome(rules, "small", 3L, "times", RoutingOutcome.Skips);
        AssertOutcome(rules, "precise", 9007199254740992d, "eq-double", RoutingOutcome.Receives);
    }

    private static void AssertOutcome(IReadOnlyList<SubscriptionRules> rules, string id, object value, string subscription, RoutingOutcome expected)
    {
        var result = TopicRouting.Route("numeric", rules, Message(id, value))
            .Subscriptions.Single(item => item.Subscription == subscription);
        Assert.Equal(expected, result.Outcome);
    }

    private static RoutingMessage Message(string id, object value) =>
        new(new EditableMessageProperties(MessageId: id), new Dictionary<string, object?> { ["id"] = value });
}
