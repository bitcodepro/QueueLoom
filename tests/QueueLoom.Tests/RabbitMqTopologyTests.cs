using System.Text.Json;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.RabbitMq;

namespace QueueLoom.Tests;

public sealed class RabbitMqTopologyTests
{
    [Theory]
    [InlineData("order.*", "order.created", true)]
    [InlineData("order.*", "order.created.eu", false)]
    [InlineData("order.#", "order", true)]
    [InlineData("order.#", "order.created.eu", true)]
    [InlineData("#.failed", "payment.card.failed", true)]
    [InlineData("#", "anything.at.all", true)]
    [InlineData("invoice.*", "order.created", false)]
    public void TopicBindings_MatchLikeRabbitMq(string pattern, string routingKey, bool expected) =>
        Assert.Equal(expected, RabbitMqTopologyIndex.TopicMatches(pattern, routingKey));

    [Fact]
    public void DeadLetterQueues_AreFoundThroughEveryKindOfExchange()
    {
        var index = new RabbitMqTopologyIndex(
            [
                Queue("by-default-exchange", """{"x-dead-letter-exchange":"","x-dead-letter-routing-key":"parking"}"""),
                Queue("by-direct", """{"x-dead-letter-exchange":"dlx-direct","x-dead-letter-routing-key":"failed"}"""),
                Queue("by-topic", """{"x-dead-letter-exchange":"dlx-topic"}"""),
                Queue("by-fanout", "{}", """{"dead-letter-exchange":"dlx-fanout"}"""),
                Queue("to-nowhere", """{"x-dead-letter-exchange":"dlx-direct","x-dead-letter-routing-key":"unknown"}"""),
                Queue("parking", "{}"), Queue("direct-dlq", "{}"), Queue("topic-dlq", "{}"), Queue("fanout-dlq", "{}")
            ],
            [new("dlx-direct", "direct"), new("dlx-topic", "topic"), new("dlx-fanout", "fanout"), new("amq.direct", "direct")],
            [new("dlx-direct", "direct-dlq", "failed"), new("dlx-topic", "topic-dlq", "*"), new("dlx-fanout", "fanout-dlq", "")]);

        Assert.Equal("parking", index.DeadLetterQueueOf("by-default-exchange"));
        Assert.Equal("direct-dlq", index.DeadLetterQueueOf("by-direct"));
        Assert.Equal("topic-dlq", index.DeadLetterQueueOf("by-topic"));
        Assert.Equal("fanout-dlq", index.DeadLetterQueueOf("by-fanout"));
        Assert.Null(index.DeadLetterQueueOf("to-nowhere"));

        var topology = index.ToTopology(DateTimeOffset.UtcNow);
        Assert.Equal("exchange", topology.TopicKindName);
        Assert.Equal(["dlx-direct", "dlx-fanout", "dlx-topic"], topology.Topics.Select(topic => topic.Name));
        Assert.Equal("direct exchange for dead letters · routes to direct-dlq", topology.Topics[0].Note);
        Assert.Equal("Dead-letter exchange dlx-direct routes to no queue", topology.Queues.Single(queue => queue.Name == "to-nowhere").Note);
    }

    [Fact]
    public void Profiles_NeedAHostAndAUser()
    {
        ServiceBusProfile Rabbit(RabbitMqSettings settings) => ServiceBusProfile.CreateNew(
                "Billing", EnvironmentKind.Test, new AuthenticationSettings(AuthenticationKind.RabbitMqPassword)) with
            { Provider = MessagingProvider.RabbitMq, RabbitMq = settings };

        Assert.True(ProfileValidator.Validate(Rabbit(new RabbitMqSettings("rabbit.internal", "queueloom"))).IsValid);
        Assert.Contains(ProfileValidator.Validate(Rabbit(new RabbitMqSettings("amqp://rabbit:5672", "queueloom"))).Errors,
            error => error.Code == "profile.rabbitmq.host.invalid");
        Assert.Contains(ProfileValidator.Validate(Rabbit(new RabbitMqSettings("rabbit", ""))).Errors,
            error => error.Code == "profile.rabbitmq.user.required");
    }

    [Fact]
    public void Editor_BuildsARabbitMqEnvironmentAndMovesPortsWithTls()
    {
        var editor = new ProfileEditorViewModel(null)
        {
            Name = "Billing",
            RabbitHost = " rabbit.internal ",
            RabbitUserName = "queueloom",
            BrokerPassword = "s3cret"
        };
        editor.SelectedProvider = editor.ProviderOptions.Single(option => option.Provider == MessagingProvider.RabbitMq);
        editor.RabbitUseTls = true;

        Assert.True(editor.TryBuild(out var result), editor.Error);
        Assert.Equal(new RabbitMqSettings("rabbit.internal", "queueloom", "/", 5671, 15671, true), result!.Profile.RabbitMq);
        Assert.Equal("s3cret", result.ConnectionString);
        Assert.Equal("rabbit.internal:5671", result.Profile.EndpointDisplay);
    }

    [Fact]
    public void ConnectedConsumers_AreShownOnTheQueue()
    {
        using var document = JsonDocument.Parse("""{"name":"orders","type":"classic","messages_ready":4,"consumers":0}""");
        var queue = RabbitQueueInfo.From(document.RootElement);
        var topology = new RabbitMqTopologyIndex([queue, Queue("idle", "{}")], [], []).ToTopology(DateTimeOffset.UnixEpoch);

        var orders = topology.Queues.Single(item => item.Name == "orders");
        Assert.Equal(0, orders.Consumers!.Consumers);
        Assert.Equal("no consumers", orders.Consumers.Summary);
        Assert.Null(topology.Queues.Single(item => item.Name == "idle").Consumers);
        var row = new EntityItemViewModel(orders.Reference, orders.Runtime, orders.Status, false, 0, consumers: orders.Consumers);
        Assert.True(row.HasConsumerLag);
    }

    [Fact]
    public void ConsumerGroupLag_IsSummarisedByTheGroupFurthestBehind()
    {
        var activity = new Core.ServiceBus.ConsumerActivity(null,
            [new("billing", 1204, "Stable"), new("audit", 0, "Empty"), new("search", 12)]);

        Assert.Equal(1204, activity.MaximumLag);
        Assert.Equal($"3 groups, lag up to {1204:N0} (billing)", activity.Summary);
        Assert.StartsWith($"billing: lag {1204:N0} (Stable)", activity.Details, StringComparison.Ordinal);
        Assert.Equal("lag 5 (a)", new Core.ServiceBus.ConsumerActivity(null, [new("a", 5)]).Summary);
    }

    private static RabbitQueueInfo Queue(string name, string arguments, string policy = "{}")
    {
        using var document = JsonDocument.Parse(
            $$"""{"name":"{{name}}","type":"classic","messages_ready":1,"arguments":{{arguments}},"effective_policy_definition":{{policy}}}""");
        return RabbitQueueInfo.From(document.RootElement);
    }
}
