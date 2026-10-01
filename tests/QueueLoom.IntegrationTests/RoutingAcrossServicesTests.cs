using System.Net.Http.Headers;
using Amazon.Runtime;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Aws;
using QueueLoom.Infrastructure.Google;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Infrastructure.RabbitMq;
using RabbitMQ.Client;
using Encoding = System.Text.Encoding;
using SendMessageRequest = QueueLoom.Core.ServiceBus.SendMessageRequest;
using Sns = Amazon.SimpleNotificationService.Model;

namespace QueueLoom.IntegrationTests;

/// <summary>Messages and checks shared by the routing tests of SNS, Pub/Sub and RabbitMQ.</summary>
internal static class RoutingProof
{
    public static MessageDraft Draft(string id, string? subject, params (string Name, object Value)[] properties) =>
        new(new EditableMessageBody("{}", MessageBodyFormat.Json), new EditableMessageProperties(MessageId: id, Subject: subject),
            properties.Select(property => ApplicationPropertyValues.FromObject(property.Name, property.Value)).ToArray());

    /// <summary>Every subscription QueueLoom says receives a message got it, and no other did.</summary>
    public static void AssertPredictionsMatch(string topic, IReadOnlyList<SubscriptionRules> rules, RoutingService service,
        IEnumerable<MessageDraft> messages, IReadOnlyDictionary<string, HashSet<string>> delivered)
    {
        foreach (var draft in messages)
        {
            var routing = TopicRouting.Route(topic, rules, RoutingMessage.From(draft), service);
            foreach (var subscription in routing.Subscriptions)
            {
                var actual = delivered[subscription.Subscription].Contains(draft.Properties.MessageId!);
                Assert.True(subscription.Outcome != RoutingOutcome.Unknown, $"{draft.Properties.MessageId} → {subscription.Subscription}: {subscription.Summary}");
                Assert.True(actual == (subscription.Outcome == RoutingOutcome.Receives),
                    $"{draft.Properties.MessageId} → {subscription.Subscription}: {service.Name()} {(actual ? "delivered" : "did not deliver")}, " +
                    $"QueueLoom said {subscription.Summary}");
            }
        }
    }
}

/// <summary>SNS filter policies against LocalStack: QueueLoom's prediction for each message is what SNS delivers.</summary>
public sealed class SnsFilterPolicyTests : IAsyncLifetime
{
    private const string Region = "eu-west-1";
    private readonly TemporaryDirectory _directory = new();
    private readonly InMemorySecretVault _vault = new();
    private readonly string _topic = Emulators.Unique("routing");
    private readonly Dictionary<string, string> _queueUrls = [];
    private AmazonSQSClient _sqs = null!;
    private AmazonSimpleNotificationServiceClient _sns = null!;
    private AwsSqsSnsWorkspace _workspace = null!;

    // Cases where LocalStack and AWS are known to agree; comparing numbers with text is left to AWS.
    private static readonly (string Name, string? Policy, bool OnBody)[] Subscriptions =
    [
        ("exact", """{"region": ["EU"]}""", false),
        ("anything-but", """{"region": [{"anything-but": ["EU", "US"]}]}""", false),
        ("prefix", """{"region": [{"prefix": "E"}]}""", false),
        ("ignore-case", """{"region": [{"equals-ignore-case": "eu"}]}""", false),
        ("range", """{"amount": [{"numeric": [">", 100, "<=", 500]}]}""", false),
        ("exists", """{"region": [{"exists": true}]}""", false),
        ("missing", """{"region": [{"exists": false}]}""", false),
        ("either", """{"$or": [{"region": ["EU"]}, {"amount": [{"numeric": [">", 1000]}]}]}""", false),
        ("name-case", """{"Region": ["EU"]}""", false),
        ("subject", """{"Subject": [{"prefix": "order."}]}""", false),
        ("body", """{"order": {"status": ["failed"]}}""", true),
        ("everything", null, false)
    ];

    public async ValueTask InitializeAsync()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(Emulators.LocalStack)))
        {
            return;
        }

        var credentials = new BasicAWSCredentials("test", "test");
        _sqs = new AmazonSQSClient(credentials, new AmazonSQSConfig { ServiceURL = Emulators.LocalStackUrl, AuthenticationRegion = Region });
        _sns = new AmazonSimpleNotificationServiceClient(credentials,
            new AmazonSimpleNotificationServiceConfig { ServiceURL = Emulators.LocalStackUrl, AuthenticationRegion = Region });
        var topicArn = (await _sns.CreateTopicAsync(_topic)).TopicArn;
        foreach (var (name, policy, onBody) in Subscriptions)
        {
            var url = (await _sqs.CreateQueueAsync($"{_topic}-{name}")).QueueUrl;
            _queueUrls[name] = url;
            var arn = (await _sqs.GetQueueAttributesAsync(url, ["QueueArn"])).Attributes["QueueArn"];
            var attributes = new Dictionary<string, string> { ["RawMessageDelivery"] = "true" };
            if (policy is not null)
            {
                attributes["FilterPolicy"] = policy;
                attributes["FilterPolicyScope"] = onBody ? "MessageBody" : "MessageAttributes";
            }
            await _sns.SubscribeAsync(new Sns.SubscribeRequest
            {
                TopicArn = topicArn, Protocol = "sqs", Endpoint = arn, Attributes = attributes, ReturnSubscriptionArn = true
            });
        }

        var profile = ServiceBusProfile.CreateNew("LocalStack", EnvironmentKind.Development,
                new AuthenticationSettings(AuthenticationKind.AwsAccessKey), accessMode: ProfileAccessMode.ReadWrite)
            with
            {
                Provider = MessagingProvider.AmazonSqsSns,
                Aws = new AwsSettings(Region, Emulators.LocalStackUrl),
                AllowQueueManagement = true
            };
        await _vault.StoreAsync(ProfileSecretKey.ConnectionString(profile.Id), new AwsAccessKey("test", "test").ToSecret());
        _workspace = new AwsSqsSnsWorkspace(_vault, backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(_directory.Path)));
        await _workspace.ConnectAsync(profile);
        await _workspace.GetTopologyAsync(forceRefresh: true);
    }

    public async ValueTask DisposeAsync()
    {
        if (_workspace is not null)
        {
            await _workspace.DisposeAsync();
        }
        _sqs?.Dispose();
        _sns?.Dispose();
        _directory.Dispose();
    }

    private string SubscriptionName(string queue) => $"sqs:{_topic}-{queue}";

    [EmulatorFact(Emulators.LocalStack)]
    public async Task Policies_are_read_and_the_prediction_matches_what_SNS_delivers()
    {
        var rules = await _workspace.GetTopicRulesAsync(_topic);
        Assert.Equal(RoutingService.Sns, _workspace.RoutingService);
        Assert.Equal(Subscriptions.Length, rules.Count);
        Assert.Empty(rules.Single(item => item.Subscription == SubscriptionName("everything")).Rules);
        var body = rules.Single(item => item.Subscription == SubscriptionName("body")).Rules.Single();
        Assert.True(body.OnMessageBody);
        Assert.Equal(RuleFilterKind.SnsFilterPolicy, body.Kind);

        MessageDraft[] messages =
        [
            RoutingProof.Draft("m1", "order.created", ("region", "EU"), ("amount", 250L)),
            RoutingProof.Draft("m2", "invoice.sent", ("region", "eu"), ("amount", 50L)),
            RoutingProof.Draft("m3", null, ("amount", 2000L)),
            RoutingProof.Draft("m4", "order.cancelled", ("region", "US"), ("amount", 101.5)),
            RoutingProof.Draft("m5", null, ("Region", "EU")),
            RoutingProof.Draft("m6", "order.x", ("region", "Asia")),
            new MessageDraft(new EditableMessageBody("""{"order": {"status": "failed"}}""", MessageBodyFormat.Json),
                new EditableMessageProperties(MessageId: "m7"), [])
        ];
        foreach (var message in messages)
        {
            await _workspace.SendMessageAsync(new SendMessageRequest(ServiceBusEntityReference.Topic(_topic), message));
        }

        await Task.Delay(TimeSpan.FromSeconds(2));
        var delivered = new Dictionary<string, HashSet<string>>();
        foreach (var (name, _, _) in Subscriptions)
        {
            var ids = new HashSet<string>();
            while (true)
            {
                var received = (await _sqs.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = _queueUrls[name], MaxNumberOfMessages = 10, MessageAttributeNames = ["All"], VisibilityTimeout = 60
                })).Messages ?? [];
                if (received.Count == 0)
                {
                    break;
                }
                foreach (var message in received)
                {
                    // SNS does not carry QueueLoom's message ID; the body and attributes tell the messages apart.
                    ids.Add(messages.Single(draft => SameMessage(draft, message)).Properties.MessageId!);
                }
            }
            delivered[SubscriptionName(name)] = ids;
        }

        RoutingProof.AssertPredictionsMatch(_topic, rules, RoutingService.Sns, messages, delivered);
    }

    private static bool SameMessage(MessageDraft draft, Message message)
    {
        if (draft.Body.Content != message.Body)
        {
            return false;
        }
        var attributes = (message.MessageAttributes ?? []).ToDictionary(pair => pair.Key, pair => pair.Value.StringValue, StringComparer.Ordinal);
        return draft.ApplicationProperties.All(property => attributes.GetValueOrDefault(property.Name) == property.Value) &&
               attributes.GetValueOrDefault("Subject") == draft.Properties.Subject &&
               draft.ApplicationProperties.Count + (draft.Properties.Subject is null ? 0 : 1) == attributes.Count;
    }

    [EmulatorFact(Emulators.LocalStack)]
    public async Task The_scope_changes_in_an_order_SNS_accepts()
    {
        var name = SubscriptionName("everything");
        var policy = new SubscriptionRule(AwsSqsSnsWorkspace.FilterPolicyRule, RuleFilterKind.SnsFilterPolicy);
        async Task<SubscriptionRule?> Read() => (await _workspace.GetTopicRulesAsync(_topic)).Single(item => item.Subscription == name).Rules.SingleOrDefault();

        // The first policy of an unfiltered subscription nests, so it needs the MessageBody scope first.
        await _workspace.SaveSubscriptionRuleAsync(_topic, name, policy with { Expression = """{"order":{"status":["failed"]}}""", OnMessageBody = true }, replace: false);
        Assert.True((await Read())!.OnMessageBody);
        Assert.Contains("failed", (await Read())!.Expression, StringComparison.Ordinal);

        await _workspace.SaveSubscriptionRuleAsync(_topic, name, policy with { Expression = """{"region":["EU"]}""", OnMessageBody = false }, replace: true);
        Assert.False((await Read())!.OnMessageBody);
        Assert.Contains("region", (await Read())!.Expression, StringComparison.Ordinal);

        await _workspace.SaveSubscriptionRuleAsync(_topic, name, policy with { Expression = """{"order":{"region":["EU"]}}""", OnMessageBody = true }, replace: true);
        Assert.True((await Read())!.OnMessageBody);
        Assert.Contains("order", (await Read())!.Expression, StringComparison.Ordinal);

        await _workspace.DeleteSubscriptionRuleAsync(_topic, name, (await Read())!);
        Assert.Null(await Read());
    }

    [EmulatorFact(Emulators.LocalStack)]
    public async Task A_policy_is_added_changed_and_removed()
    {
        var name = SubscriptionName("everything");
        var policy = new SubscriptionRule(AwsSqsSnsWorkspace.FilterPolicyRule, RuleFilterKind.SnsFilterPolicy) { Expression = """{"tier":["gold"]}""" };
        await _workspace.SaveSubscriptionRuleAsync(_topic, name, policy, replace: false);
        var saved = (await _workspace.GetTopicRulesAsync(_topic)).Single(item => item.Subscription == name).Rules.Single();
        Assert.Contains("gold", saved.Expression, StringComparison.Ordinal);
        Assert.False(saved.OnMessageBody);

        await _workspace.SaveSubscriptionRuleAsync(_topic, name, policy with { Expression = """{"order":{"tier":["gold"]}}""", OnMessageBody = true }, replace: true);
        saved = (await _workspace.GetTopicRulesAsync(_topic)).Single(item => item.Subscription == name).Rules.Single();
        Assert.True(saved.OnMessageBody);
        Assert.Contains("order", saved.Expression, StringComparison.Ordinal);

        await Assert.ThrowsAsync<FilterPolicyException>(() =>
            _workspace.SaveSubscriptionRuleAsync(_topic, name, policy with { Expression = """{"tier":"gold"}""", OnMessageBody = false }, replace: true));

        await _workspace.DeleteSubscriptionRuleAsync(_topic, name, saved.Name);
        Assert.Empty((await _workspace.GetTopicRulesAsync(_topic)).Single(item => item.Subscription == name).Rules);
    }
}

/// <summary>Pub/Sub filters against the emulator: QueueLoom's prediction for each message is what Pub/Sub delivers.</summary>
public sealed class PubSubFilterTests : IAsyncLifetime
{
    private const string Project = "queueloom-test";
    private readonly TemporaryDirectory _directory = new();
    private readonly string _topic = Emulators.Unique("routing");
    private SubscriberServiceApiClient _subscriber = null!;
    private GooglePubSubWorkspace _workspace = null!;

    private static readonly (string Name, string Filter)[] Subscriptions =
    [
        ("equal", "attributes.region = \"EU\""),
        ("not-equal", "attributes.region != \"EU\""),
        ("has", "attributes:region"),
        ("has-not", "NOT attributes:region"),
        ("prefix", "hasPrefix(attributes.Subject, \"order.\")"),
        ("both", "attributes.region = \"EU\" AND (attributes.tier = \"gold\" OR attributes.tier = \"silver\")"),
        ("quoted", "attributes.\"my-key\" = \"x\""),
        ("name-case", "attributes.Region = \"EU\""),
        ("number", "attributes.amount = \"250\""),
        ("everything", "")
    ];

    private string Name(string subscription) => $"{_topic}-{subscription}";

    public async ValueTask InitializeAsync()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(Emulators.PubSub)))
        {
            return;
        }

        var publisher = await new PublisherServiceApiClientBuilder { Endpoint = Emulators.PubSubHost, ChannelCredentials = ChannelCredentials.Insecure }
            .BuildAsync();
        _subscriber = await new SubscriberServiceApiClientBuilder { Endpoint = Emulators.PubSubHost, ChannelCredentials = ChannelCredentials.Insecure }
            .BuildAsync();
        await publisher.CreateTopicAsync(new TopicName(Project, _topic));
        foreach (var (name, filter) in Subscriptions)
        {
            await _subscriber.CreateSubscriptionAsync(new Subscription
            {
                SubscriptionName = new SubscriptionName(Project, Name(name)),
                TopicAsTopicName = new TopicName(Project, _topic),
                AckDeadlineSeconds = 10,
                Filter = filter
            });
        }

        var profile = ServiceBusProfile.CreateNew("Pub/Sub emulator", EnvironmentKind.Development,
                new AuthenticationSettings(AuthenticationKind.GoogleApplicationDefault), accessMode: ProfileAccessMode.ReadWrite)
            with
            {
                Provider = MessagingProvider.GooglePubSub,
                GooglePubSub = new GooglePubSubSettings(Project, Emulators.PubSubHost),
                AllowQueueManagement = true
            };
        _workspace = new GooglePubSubWorkspace(new InMemorySecretVault(),
            backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(_directory.Path)));
        await _workspace.ConnectAsync(profile);
        await _workspace.GetTopologyAsync(forceRefresh: true);
    }

    public async ValueTask DisposeAsync()
    {
        if (_workspace is not null)
        {
            await _workspace.DisposeAsync();
        }
        _directory.Dispose();
    }

    [EmulatorFact(Emulators.PubSub)]
    public async Task Filters_are_read_and_the_prediction_matches_what_Pub_Sub_delivers()
    {
        var rules = await _workspace.GetTopicRulesAsync(_topic);
        Assert.Equal(Subscriptions.Select(item => Name(item.Name)).Order(StringComparer.Ordinal), rules.Select(item => item.Subscription));
        Assert.Empty(rules.Single(item => item.Subscription == Name("everything")).Rules);
        Assert.NotNull(_workspace.RuleEditingNote);

        MessageDraft[] messages =
        [
            RoutingProof.Draft("m1", "order.created", ("region", "EU"), ("tier", "gold")),
            RoutingProof.Draft("m2", "invoice.sent", ("region", "eu")),
            RoutingProof.Draft("m3", null, ("amount", 250L)),
            RoutingProof.Draft("m4", "order.cancelled", ("Region", "EU"), ("tier", "silver")),
            RoutingProof.Draft("m5", null, ("region", "")),
            RoutingProof.Draft("m6", null, ("my-key", "x"), ("region", "EU"), ("tier", "bronze"))
        ];
        foreach (var message in messages)
        {
            await _workspace.SendMessageAsync(new SendMessageRequest(ServiceBusEntityReference.Topic(_topic), message));
        }

        var delivered = new Dictionary<string, HashSet<string>>();
        foreach (var (name, _) in Subscriptions)
        {
            var ids = new HashSet<string>();
            for (var attempt = 0; attempt < 2; attempt++)
            {
                PullResponse response;
                try
                {
                    // An empty subscription would hold the pull open, so each pull gets a second.
                    response = await _subscriber.PullAsync(new PullRequest
                    {
                        SubscriptionAsSubscriptionName = new SubscriptionName(Project, Name(name)), MaxMessages = 50
                    }, CallSettings.FromExpiration(Expiration.FromTimeout(TimeSpan.FromSeconds(1))));
                }
                catch (RpcException exception) when (exception.StatusCode is StatusCode.DeadlineExceeded or StatusCode.Cancelled)
                {
                    continue;
                }
                foreach (var received in response.ReceivedMessages)
                {
                    ids.Add(messages.Single(draft => SameMessage(draft, received.Message)).Properties.MessageId!);
                }
                if (response.ReceivedMessages.Count > 0)
                {
                    await _subscriber.AcknowledgeAsync(new SubscriptionName(Project, Name(name)), response.ReceivedMessages.Select(item => item.AckId));
                }
            }
            delivered[Name(name)] = ids;
        }

        RoutingProof.AssertPredictionsMatch(_topic, rules, RoutingService.PubSub, messages, delivered);
    }

    private static bool SameMessage(MessageDraft draft, PubsubMessage message) =>
        draft.ApplicationProperties.Count + (draft.Properties.Subject is null ? 0 : 1) == message.Attributes.Count &&
        draft.ApplicationProperties.All(property => message.Attributes.TryGetValue(property.Name, out var value) && value == property.Value) &&
        (draft.Properties.Subject is null || message.Attributes.TryGetValue("Subject", out var subject) && subject == draft.Properties.Subject);
}

/// <summary>RabbitMQ bindings against a real broker: QueueLoom's prediction for each message is where RabbitMQ routes it.</summary>
public sealed class RabbitMqBindingTests : IAsyncLifetime
{
    private readonly TemporaryDirectory _directory = new();
    private readonly InMemorySecretVault _vault = new();
    private readonly string _vhost = Emulators.Unique("routing");
    private HttpClient _management = null!;
    private IConnection _connection = null!;
    private IChannel _setup = null!;
    private RabbitMqWorkspace _workspace = null!;

    public async ValueTask InitializeAsync()
    {
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(Emulators.RabbitMq)))
        {
            return;
        }

        _management = new HttpClient(new HttpClientHandler { UseProxy = false })
        {
            BaseAddress = new Uri($"http://{Emulators.RabbitMqHost}:{Emulators.RabbitMqPort + 10000}/")
        };
        _management.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String("guest:guest"u8.ToArray()));
        (await _management.PutAsync($"api/vhosts/{_vhost}", null)).EnsureSuccessStatusCode();
        (await _management.PutAsync($"api/permissions/{_vhost}/guest",
            new StringContent("""{"configure":".*","write":".*","read":".*"}""", Encoding.UTF8, "application/json"))).EnsureSuccessStatusCode();
        _connection = await new ConnectionFactory { HostName = Emulators.RabbitMqHost, Port = Emulators.RabbitMqPort, VirtualHost = _vhost }
            .CreateConnectionAsync();
        _setup = await _connection.CreateChannelAsync();

        await _setup.ExchangeDeclareAsync("unrouted", ExchangeType.Fanout, durable: true);
        await _setup.ExchangeDeclareAsync("events", ExchangeType.Topic, durable: true,
            arguments: new Dictionary<string, object?> { ["alternate-exchange"] = "unrouted" });
        await _setup.ExchangeDeclareAsync("by-header", ExchangeType.Headers, durable: true);
        foreach (var queue in new[] { "orders", "all-events", "eu-orders", "gold", "any-tier", "amount", "leftovers", "mirror" })
        {
            await _setup.QueueDeclareAsync(queue, durable: true, exclusive: false, autoDelete: false);
        }
        await _setup.QueueBindAsync("orders", "events", "order.*");
        await _setup.QueueBindAsync("all-events", "events", "#");
        await _setup.QueueBindAsync("eu-orders", "events", "order.eu.#");
        await _setup.QueueBindAsync("eu-orders", "events", "*.eu");
        await _setup.QueueBindAsync("leftovers", "unrouted", string.Empty);
        await _setup.ExchangeBindAsync("by-header", "events", "audit.#");
        await _setup.QueueBindAsync("gold", "by-header", string.Empty, new Dictionary<string, object?>
        {
            ["x-match"] = "all", ["region"] = "EU", ["tier"] = "gold"
        });
        await _setup.QueueBindAsync("any-tier", "by-header", string.Empty, new Dictionary<string, object?>
        {
            ["x-match"] = "any", ["tier"] = "gold", ["vip"] = true
        });
        await _setup.QueueBindAsync("amount", "by-header", string.Empty, new Dictionary<string, object?> { ["amount"] = 250L });

        var profile = ServiceBusProfile.CreateNew("RabbitMQ", EnvironmentKind.Development,
                new AuthenticationSettings(AuthenticationKind.RabbitMqPassword), accessMode: ProfileAccessMode.ReadWrite) with
        {
            Provider = MessagingProvider.RabbitMq,
            RabbitMq = new RabbitMqSettings(Emulators.RabbitMqHost, "guest", _vhost, Emulators.RabbitMqPort, Emulators.RabbitMqPort + 10000),
            AllowQueueManagement = true
        };
        await _vault.StoreAsync(ProfileSecretKey.ConnectionString(profile.Id), "guest");
        _workspace = new RabbitMqWorkspace(_vault, backupStore: new DeadLetterJsonBackupStore(QueueLoomPaths.ForRoot(_directory.Path)),
            httpHandler: new HttpClientHandler { UseProxy = false });
        await _workspace.ConnectAsync(profile);
        await _workspace.GetTopologyAsync(forceRefresh: true);
    }

    public async ValueTask DisposeAsync()
    {
        if (_workspace is not null)
        {
            await _workspace.DisposeAsync();
        }
        if (_connection is not null)
        {
            await _setup.DisposeAsync();
            await _connection.DisposeAsync();
            await _management.DeleteAsync($"api/vhosts/{_vhost}");
            _management.Dispose();
        }
        _directory.Dispose();
    }

    [EmulatorFact(Emulators.RabbitMq)]
    public async Task Bindings_are_read_and_the_prediction_matches_where_RabbitMQ_routes()
    {
        var events = await _workspace.GetTopicRulesAsync("events");
        Assert.Equal(["all-events", "by-header", "eu-orders", "orders", "unrouted"], events.Select(item => item.Subscription));
        Assert.True(events.Single(item => item.Subscription == "unrouted").IsFallback);
        Assert.True(events.Single(item => item.Subscription == "by-header").IsExchange);
        Assert.Equal(2, events.Single(item => item.Subscription == "eu-orders").Rules.Count);
        Assert.All(events.Single(item => item.Subscription == "orders").Rules, rule => Assert.Equal(RuleFilterKind.TopicBinding, rule.Kind));

        var headers = await _workspace.GetTopicRulesAsync("by-header");
        Assert.Equal(["amount", "any-tier", "gold"], headers.Select(item => item.Subscription));
        Assert.Equal(250L, headers.Single(item => item.Subscription == "amount").Rules.Single().Arguments["amount"]);

        // Through the topic exchange: what each binding and the alternate exchange get.
        MessageDraft[] topicMessages =
        [
            RoutingProof.Draft("t1", "order.created"),
            RoutingProof.Draft("t2", "order.eu.created"),
            RoutingProof.Draft("t3", "invoice.eu")
        ];
        var unroutedTopic = await _workspace.GetTopicRulesAsync("events");
        // "#" takes everything, so the alternate exchange gets nothing here; the case without it is checked below.
        await SendAllAsync("events", topicMessages);
        var deliveredTopic = new Dictionary<string, HashSet<string>>
        {
            ["orders"] = await DrainAsync("orders"),
            ["all-events"] = await DrainAsync("all-events"),
            ["eu-orders"] = await DrainAsync("eu-orders"),
            ["unrouted"] = await DrainAsync("leftovers"),
            ["by-header"] = []
        };
        RoutingProof.AssertPredictionsMatch("events", unroutedTopic, RoutingService.RabbitMq, topicMessages, deliveredTopic);

        // Through the headers exchange: the type of a header value counts, and names are case-sensitive.
        MessageDraft[] headerMessages =
        [
            RoutingProof.Draft("h1", null, ("region", "EU"), ("tier", "gold")),
            RoutingProof.Draft("h2", null, ("region", "EU")),
            RoutingProof.Draft("h3", null, ("amount", 250L)),
            RoutingProof.Draft("h4", null, ("amount", "250")),
            RoutingProof.Draft("h5", null, ("Region", "EU"), ("tier", "silver"), ("vip", true)),
            RoutingProof.Draft("h6", null, ("amount", 250.0)),
            RoutingProof.Draft("h7", null, ("amount", 250))
        ];
        await SendAllAsync("by-header", headerMessages);
        var deliveredHeaders = new Dictionary<string, HashSet<string>>
        {
            ["amount"] = await DrainAsync("amount"),
            ["any-tier"] = await DrainAsync("any-tier"),
            ["gold"] = await DrainAsync("gold")
        };
        RoutingProof.AssertPredictionsMatch("by-header", headers, RoutingService.RabbitMq, headerMessages, deliveredHeaders);
    }

    [EmulatorFact(Emulators.RabbitMq)]
    public async Task The_alternate_exchange_gets_what_no_binding_takes()
    {
        var all = (await _workspace.GetTopicRulesAsync("events")).Single(item => item.Subscription == "all-events").Rules.Single();
        await _workspace.DeleteSubscriptionRuleAsync("events", "all-events", all.Name);
        var rules = await _workspace.GetTopicRulesAsync("events");
        Assert.DoesNotContain(rules, item => item.Subscription == "all-events");

        MessageDraft[] messages = [RoutingProof.Draft("a1", "order.created"), RoutingProof.Draft("a2", "shipment.sent")];
        await SendAllAsync("events", messages);
        var delivered = new Dictionary<string, HashSet<string>>
        {
            ["orders"] = await DrainAsync("orders"),
            ["eu-orders"] = await DrainAsync("eu-orders"),
            ["unrouted"] = await DrainAsync("leftovers"),
            ["by-header"] = []
        };
        RoutingProof.AssertPredictionsMatch("events", rules, RoutingService.RabbitMq, messages, delivered);
        Assert.Equal(["a2"], delivered["unrouted"]);
    }

    [EmulatorFact(Emulators.RabbitMq)]
    public async Task A_queue_and_an_exchange_of_the_same_name_keep_their_own_bindings()
    {
        await _setup.ExchangeDeclareAsync("same", ExchangeType.Fanout, durable: true);
        await _setup.QueueDeclareAsync("same", durable: true, exclusive: false, autoDelete: false);
        await _setup.QueueBindAsync("same", "events", "same.key");
        await _setup.ExchangeBindAsync("same", "events", "same.key");
        await _workspace.GetTopologyAsync(forceRefresh: true);

        var rules = (await _workspace.GetTopicRulesAsync("events")).Where(item => item.Subscription == "same").ToArray();
        Assert.Equal(2, rules.Length);
        var exchange = rules.Single(item => item.IsExchange);
        Assert.True(exchange.Rules.Single().ToExchange);
        Assert.False(rules.Single(item => !item.IsExchange).Rules.Single().ToExchange);

        // Changing and deleting the exchange's binding leaves the queue's alone.
        await _workspace.SaveSubscriptionRuleAsync("events", "same", exchange.Rules.Single() with { Expression = "same.other" }, replace: true);
        rules = (await _workspace.GetTopicRulesAsync("events")).Where(item => item.Subscription == "same").ToArray();
        Assert.Equal("same.other", rules.Single(item => item.IsExchange).Rules.Single().Expression);
        Assert.Equal("same.key", rules.Single(item => !item.IsExchange).Rules.Single().Expression);

        await _workspace.DeleteSubscriptionRuleAsync("events", "same", rules.Single(item => item.IsExchange).Rules.Single());
        rules = (await _workspace.GetTopicRulesAsync("events")).Where(item => item.Subscription == "same").ToArray();
        Assert.Equal("same.key", Assert.Single(rules).Rules.Single().Expression);
        Assert.False(rules[0].IsExchange);

        // Without saying which one, a shared name is refused rather than guessed.
        await _setup.ExchangeBindAsync("same", "events", "same.again");
        await _workspace.GetTopologyAsync(forceRefresh: true);
        await Assert.ThrowsAsync<InvalidOperationException>(() => _workspace.DeleteSubscriptionRuleAsync("events", "same", "same.again"));
    }

    [EmulatorFact(Emulators.RabbitMq)]
    public async Task Bindings_are_added_changed_and_removed()
    {
        await _workspace.SaveSubscriptionRuleAsync("events", "orders", new SubscriptionRule(string.Empty, RuleFilterKind.TopicBinding)
        {
            Expression = "invoice.*"
        }, replace: false);
        var orders = (await _workspace.GetTopicRulesAsync("events")).Single(item => item.Subscription == "orders");
        Assert.Equal(["invoice.*", "order.*"], orders.Rules.Select(rule => rule.Expression).Order(StringComparer.Ordinal));

        var invoice = orders.Rules.Single(rule => rule.Expression == "invoice.*");
        await _workspace.SaveSubscriptionRuleAsync("events", "orders", invoice with { Expression = "invoice.#" }, replace: true);
        orders = (await _workspace.GetTopicRulesAsync("events")).Single(item => item.Subscription == "orders");
        Assert.Equal(["invoice.#", "order.*"], orders.Rules.Select(rule => rule.Expression).Order(StringComparer.Ordinal));

        await _workspace.SaveSubscriptionRuleAsync("by-header", "gold", new SubscriptionRule(string.Empty, RuleFilterKind.HeadersBinding)
        {
            Arguments = new Dictionary<string, object?> { ["x-match"] = "any", ["tier"] = "platinum", ["amount"] = 1000L }
        }, replace: false);
        var gold = (await _workspace.GetTopicRulesAsync("by-header")).Single(item => item.Subscription == "gold");
        Assert.Equal(2, gold.Rules.Count);
        Assert.Contains(gold.Rules, rule => Equals(rule.Arguments.GetValueOrDefault("amount"), 1000L));

        await _workspace.DeleteSubscriptionRuleAsync("events", "orders", orders.Rules.Single(rule => rule.Expression == "invoice.#").Name);
        Assert.Single((await _workspace.GetTopicRulesAsync("events")).Single(item => item.Subscription == "orders").Rules);
    }

    private async Task SendAllAsync(string exchange, IEnumerable<MessageDraft> messages)
    {
        foreach (var message in messages)
        {
            try
            {
                await _workspace.SendMessageAsync(new SendMessageRequest(ServiceBusEntityReference.Topic(exchange), message));
            }
            catch (InvalidOperationException)
            {
                // Unroutable: QueueLoom publishes with the mandatory flag and reports the message as not delivered.
            }
        }
    }

    private async Task<HashSet<string>> DrainAsync(string queue)
    {
        var ids = new HashSet<string>();
        await Task.Delay(200);
        while (await _setup.BasicGetAsync(queue, autoAck: true) is { } result)
        {
            ids.Add(result.BasicProperties.MessageId!);
        }
        return ids;
    }
}
