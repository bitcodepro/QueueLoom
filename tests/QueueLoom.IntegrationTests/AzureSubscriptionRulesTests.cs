using System.Diagnostics;
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
/// Subscription rules against the Service Bus emulator, and the proof that matters: for every test message, the
/// subscriptions QueueLoom predicts are exactly the ones Service Bus delivers to.
/// </summary>
public sealed class AzureSubscriptionRulesTests : IAsyncLifetime
{
    private readonly TemporaryDirectory _directory = new();
    private readonly string _topic = Emulators.Unique("routing");
    private ServiceBusAdministrationClient _administration = null!;
    private ServiceBusClient _client = null!;
    private AzureServiceBusWorkspace _workspace = null!;

    private static readonly (string Name, SubscriptionRule Rule)[] Subscriptions =
    [
        ("eu-big", new SubscriptionRule("eu-big", RuleFilterKind.Sql, "region = 'EU' AND amount > 100")),
        ("created", new SubscriptionRule("created", RuleFilterKind.Correlation, Correlation: new CorrelationFilterFields(Subject: "order.created"))),
        ("tenant", new SubscriptionRule("tenant", RuleFilterKind.Correlation, Correlation: new CorrelationFilterFields
        {
            Properties = new Dictionary<string, object> { ["tenant"] = "acme" }
        })),
        ("like", new SubscriptionRule("like", RuleFilterKind.Sql, "sys.Label LIKE 'order.%' AND NOT (region IN ('US', 'CA'))")),
        ("missing", new SubscriptionRule("missing", RuleFilterKind.Sql, "priority IS NULL OR priority <> 'low'")),
        ("case", new SubscriptionRule("case", RuleFilterKind.Sql, "region = 'eu'")),
        ("numeric", new SubscriptionRule("numeric", RuleFilterKind.Correlation, Correlation: new CorrelationFilterFields
        {
            Properties = new Dictionary<string, object> { ["amount"] = 250L }
        })),
        ("name-case-sql", new SubscriptionRule("name-case-sql", RuleFilterKind.Sql, "Region = 'EU' AND user.TENANT = 'acme'")),
        ("name-case-correlation", new SubscriptionRule("name-case-correlation", RuleFilterKind.Correlation, Correlation: new CorrelationFilterFields
        {
            Properties = new Dictionary<string, object> { ["Tenant"] = "acme" }
        })),
        ("typed", new SubscriptionRule("typed", RuleFilterKind.Correlation, Correlation: new CorrelationFilterFields
        {
            Properties = new Dictionary<string, object>
            {
                ["ratio"] = 0.5,
                ["due"] = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc)
            }
        })),
        ("int-vs-long", new SubscriptionRule("int-vs-long", RuleFilterKind.Correlation, Correlation: new CorrelationFilterFields
        {
            Properties = new Dictionary<string, object> { ["count"] = 7L }
        }))
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
        foreach (var (name, rule) in Subscriptions)
        {
            await _administration.CreateSubscriptionAsync(
                new CreateSubscriptionOptions(_topic, name) { DefaultMessageTimeToLive = TimeSpan.FromMinutes(30) },
                new CreateRuleOptions(rule.Name, AzureServiceBusWorkspace.ToFilter(rule)));
        }
        await _administration.CreateSubscriptionAsync(new CreateSubscriptionOptions(_topic, "everything") { DefaultMessageTimeToLive = TimeSpan.FromMinutes(30) });
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
    public async Task Rules_are_read_and_the_prediction_matches_what_Service_Bus_delivers()
    {
        var rules = await _workspace.GetTopicRulesAsync(_topic);
        Assert.Equal(["case", "created", "eu-big", "everything", "int-vs-long", "like", "missing", "name-case-correlation", "name-case-sql", "numeric",
            "tenant", "typed"], rules.Select(item => item.Subscription));
        Assert.IsNotType<string>(rules.Single(item => item.Subscription == "numeric").Rules.Single().Correlation!.Properties["amount"]);
        Assert.Equal(RuleFilterKind.True, rules.Single(item => item.Subscription == "everything").Rules.Single().Kind);
        Assert.Equal("acme", rules.Single(item => item.Subscription == "tenant").Rules.Single().Correlation!.Properties["tenant"]);

        MessageDraft[] messages =
        [
            Draft("m1", "order.created", ("region", "EU"), ("amount", 250L), ("tenant", "acme")),
            Draft("m2", "order.cancelled", ("region", "eu"), ("amount", 50L), ("priority", "low")),
            Draft("m3", "Order.Created", ("region", "US"), ("tenant", "ACME")),
            Draft("m4", null, ("amount", 101.5), ("priority", "high")),
            Draft("m5", "order.created", ("amount", "250"), ("region", "EU")),
            Typed("m6", new("trace", ApplicationPropertyType.Guid, "4f3c2a1b-0000-4000-8000-000000000001"),
                new("ratio", ApplicationPropertyType.Single, "0.5"), new("due", ApplicationPropertyType.DateTime, "2026-10-01T12:00:00.0000000Z"),
                new("count", ApplicationPropertyType.Int32, "7"), new("note", ApplicationPropertyType.String, "line1\nline2")),
            Typed("m7", new("trace", ApplicationPropertyType.String, "4f3c2a1b-0000-4000-8000-000000000001"),
                new("ratio", ApplicationPropertyType.Double, "0.2"), new("due", ApplicationPropertyType.DateTime, "2026-10-01T12:00:00.0000000Z"),
                new("count", ApplicationPropertyType.String, "7"))
        ];

        var sender = _client.CreateSender(_topic);
        foreach (var draft in messages)
        {
            var message = new ServiceBusMessage(BinaryData.FromString("{}")) { MessageId = draft.Properties.MessageId, Subject = draft.Properties.Subject };
            foreach (var property in draft.ApplicationProperties)
            {
                message.ApplicationProperties[property.Name] = ApplicationPropertyValues.ToObject(property);
            }
            await sender.SendMessageAsync(message);
        }

        // The broker fans a message out to its subscriptions asynchronously, so a subscription peeked right after the
        // sends can still be missing one. Peek every subscription until none has changed for two seconds (at most 30 s).
        var delivered = rules.ToDictionary(item => item.Subscription, _ => new HashSet<string>());
        var receivers = rules.Select(item => _client.CreateReceiver(_topic, item.Subscription)).ToArray();
        try
        {
            var deadline = Stopwatch.StartNew();
            var unchangedSince = Stopwatch.StartNew();
            while (unchangedSince.Elapsed < TimeSpan.FromSeconds(2) && deadline.Elapsed < TimeSpan.FromSeconds(30))
            {
                for (var index = 0; index < receivers.Length; index++)
                {
                    foreach (var message in await receivers[index].PeekMessagesAsync(50, fromSequenceNumber: 0))
                    {
                        if (delivered[rules[index].Subscription].Add(message.MessageId)) unchangedSince.Restart();
                    }
                }
                await Task.Delay(200);
            }
        }
        finally
        {
            foreach (var receiver in receivers) await receiver.DisposeAsync();
        }

        foreach (var draft in messages)
        {
            var routing = TopicRouting.Route(_topic, rules, RoutingMessage.From(draft));
            foreach (var subscription in routing.Subscriptions)
            {
                var actual = delivered[subscription.Subscription].Contains(draft.Properties.MessageId!);
                Assert.True(subscription.Outcome != RoutingOutcome.Unknown, $"{draft.Properties.MessageId} → {subscription.Subscription}: {subscription.Summary}");
                Assert.True(actual == (subscription.Outcome == RoutingOutcome.Receives),
                    $"{draft.Properties.MessageId} → {subscription.Subscription}: Service Bus {(actual ? "delivered" : "did not deliver")}, QueueLoom said {subscription.Summary}");
            }
        }
    }

    [EmulatorFact(Emulators.ServiceBus)]
    public async Task Typed_correlation_values_survive_an_edit()
    {
        var rule = new SubscriptionRule("typed", RuleFilterKind.Correlation, Correlation: new CorrelationFilterFields(Subject: "s", ReplyToSessionId: "r-1")
        {
            Properties = new Dictionary<string, object> { ["amount"] = 250L, ["vip"] = true, ["code"] = "250" }
        });
        await _workspace.SaveSubscriptionRuleAsync(_topic, "everything", rule, replace: false);
        var read = (await _workspace.GetTopicRulesAsync(_topic)).Single(item => item.Subscription == "everything").Rules.Single(item => item.Name == "typed");

        await _workspace.SaveSubscriptionRuleAsync(_topic, "everything", read with { Action = "SET seen = 1" }, replace: true);
        var edited = (await _workspace.GetTopicRulesAsync(_topic)).Single(item => item.Subscription == "everything").Rules.Single(item => item.Name == "typed");

        Assert.Equal("r-1", edited.Correlation!.ReplyToSessionId);
        Assert.True(RoutingValue.AreEqual(250L, edited.Correlation.Properties["amount"]));
        Assert.IsNotType<string>(edited.Correlation.Properties["amount"]);
        Assert.Equal(true, edited.Correlation.Properties["vip"]);
        Assert.Equal("250", edited.Correlation.Properties["code"]);
        Assert.NotNull(edited.Action);
    }

    [EmulatorFact(Emulators.ServiceBus)]
    public async Task Rules_are_added_changed_and_deleted()
    {
        var rule = new SubscriptionRule("vip", RuleFilterKind.Sql, "tier = 'gold'", Action: "SET priority = 'high'");
        await _workspace.SaveSubscriptionRuleAsync(_topic, "everything", rule, replace: false);
        var saved = (await _workspace.GetTopicRulesAsync(_topic)).Single(item => item.Subscription == "everything").Rules.Single(item => item.Name == "vip");
        Assert.Equal("tier = 'gold'", saved.SqlExpression);
        Assert.Contains("priority", saved.Action, StringComparison.OrdinalIgnoreCase);

        await _workspace.SaveSubscriptionRuleAsync(_topic, "everything", rule with { SqlExpression = "tier IN ('gold', 'platinum')", Action = null }, replace: true);
        saved = (await _workspace.GetTopicRulesAsync(_topic)).Single(item => item.Subscription == "everything").Rules.Single(item => item.Name == "vip");
        Assert.Contains("platinum", saved.SqlExpression, StringComparison.Ordinal);
        Assert.Null(saved.Action);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _workspace.SaveSubscriptionRuleAsync(_topic, "everything", new SubscriptionRule("broken", RuleFilterKind.Sql, "tier = = 'x'"), replace: false));

        await _workspace.DeleteSubscriptionRuleAsync(_topic, "everything", "vip");
        Assert.DoesNotContain((await _workspace.GetTopicRulesAsync(_topic)).Single(item => item.Subscription == "everything").Rules, item => item.Name == "vip");
    }

    private static MessageDraft Typed(string id, params MessageApplicationProperty[] properties) =>
        new(new EditableMessageBody("{}", MessageBodyFormat.Json), new EditableMessageProperties(MessageId: id), properties);

    private static MessageDraft Draft(string id, string? subject, params (string Name, object Value)[] properties) =>
        new(new EditableMessageBody("{}", MessageBodyFormat.Json), new EditableMessageProperties(MessageId: id, Subject: subject),
            properties.Select(property => new MessageApplicationProperty(property.Name, property.Value switch
            {
                long => ApplicationPropertyType.Int64,
                double => ApplicationPropertyType.Double,
                _ => ApplicationPropertyType.String
            }, Convert.ToString(property.Value, System.Globalization.CultureInfo.InvariantCulture)!)));
}
