using System.Text;
using System.Text.Json;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

/// <summary>The cases from the review of the routing, forwarding, search and Protobuf changes.</summary>
public sealed class ReviewRegressionTests
{
    private static RoutingMessage Message(string? subject = null, string? body = null, params (string Name, object Value)[] properties) =>
        new(new EditableMessageProperties(Subject: subject), properties.Select(pair => new KeyValuePair<string, object?>(pair.Name, pair.Value))) { Body = body };

    [Theory]
    [InlineData("all-with-x")]
    [InlineData("any-with-x")]
    [InlineData("all")]
    [InlineData("any")]
    public void A_headers_binding_keeps_its_match_mode_when_saved_unchanged(string mode)
    {
        var binding = new SubscriptionRule("~key", RuleFilterKind.HeadersBinding)
        {
            Arguments = new Dictionary<string, object?> { ["x-match"] = mode, ["x-tenant"] = "acme", ["region"] = "EU" }
        };
        var saved = new RuleEditorViewModel("by-header", "gold", binding, RoutingService.RabbitMq).TryBuild()!;
        Assert.Equal(mode, saved.Arguments["x-match"]);
        Assert.Equal("acme", saved.Arguments["x-tenant"]);
        var message = Message(null, null, ("region", "EU"));
        Assert.Equal(RabbitBindings.MatchHeaders(binding.Arguments, message).Outcome, RabbitBindings.MatchHeaders(saved.Arguments, message).Outcome);
    }

    [Fact]
    public void Topic_names_with_slashes_keep_their_subscriptions_apart()
    {
        var topology = new ServiceBusTopology(DateTimeOffset.UtcNow,
            [new ServiceBusQueue("out", ServiceBusEntityRuntime.Empty)],
            [
                new ServiceBusTopic("orders", ServiceBusEntityRuntime.Empty),
                new ServiceBusTopic("orders/eu", ServiceBusEntityRuntime.Empty,
                    [new ServiceBusSubscription("orders/eu", "worker", ServiceBusEntityRuntime.Empty) { ForwardTo = "out" }])
            ]);
        var report = Forwarding.Follow(topology, "orders/eu/worker");
        Assert.Empty(report.Missing);
        Assert.Equal(["out"], report.Destinations);
        Assert.Equal(1, report.LongestChain);
        Assert.Equal(["orders/eu", "orders/eu/worker", "out"], Forwarding.Follow(topology, "orders/eu").Paths.Single());
    }

    [Fact]
    public void Queues_named_like_paths_still_count_every_forward()
    {
        string[] names = ["a", "a/b", "a/b/c", "a/b/c/d", "a/b/c/d/e", "a/b/c/d/e/f"];
        var topology = new ServiceBusTopology(DateTimeOffset.UtcNow, names.Select((name, index) =>
            new ServiceBusQueue(name, ServiceBusEntityRuntime.Empty) { ForwardTo = index + 1 < names.Length ? names[index + 1] : null }));
        var report = Forwarding.Follow(topology, "a");
        Assert.Equal(5, report.LongestChain);
        Assert.True(report.IsTooLong);

        // A real topic copy is still not a forward: queue → topic → subscription → queue is two.
        var copy = Forwarding.Follow(new ServiceBusTopology(DateTimeOffset.UtcNow,
            [new ServiceBusQueue("in", ServiceBusEntityRuntime.Empty) { ForwardTo = "t" }, new ServiceBusQueue("end", ServiceBusEntityRuntime.Empty)],
            [new ServiceBusTopic("t", ServiceBusEntityRuntime.Empty, [new ServiceBusSubscription("t", "s", ServiceBusEntityRuntime.Empty) { ForwardTo = "end" }])]), "in");
        Assert.Equal(2, copy.LongestChain);
    }

    [Fact]
    public void SNS_body_policies_look_into_arrays_of_objects()
    {
        const string Policy = """{"Records":{"eventName":["ObjectCreated:Put"]}}""";
        Assert.Equal(RoutingOutcome.Receives,
            SnsFilterPolicy.Evaluate(Policy, true, Message(body: """{"Records":[{"eventName":"ObjectCreated:Copy"},{"eventName":"ObjectCreated:Put"}]}""")).Outcome);
        Assert.Equal(RoutingOutcome.Skips,
            SnsFilterPolicy.Evaluate(Policy, true, Message(body: """{"Records":[{"eventName":"ObjectRemoved:Delete"}]}""")).Outcome);
        Assert.Equal(RoutingOutcome.Receives,
            SnsFilterPolicy.Evaluate("""{"Records":{"s3":{"bucket":{"name":[{"prefix":"logs-"}]}}}}""", true,
                Message(body: """{"Records":[{"s3":{"bucket":{"name":"logs-eu"}}}]}""")).Outcome);
    }

    [Fact]
    public void The_alternate_exchange_is_not_ruled_out_by_an_exchange_that_may_reach_no_queue()
    {
        SubscriptionRules[] bindings =
        [
            new("relay", [new SubscriptionRule("#", RuleFilterKind.TopicBinding) { Expression = "#" }]) { Service = RoutingService.RabbitMq, IsExchange = true },
            new("unrouted", []) { Service = RoutingService.RabbitMq, IsFallback = true, IsExchange = true }
        ];
        var routed = TopicRouting.Route("events", bindings, Message("order.created"), RoutingService.RabbitMq);
        Assert.Equal(RoutingOutcome.Receives, routed.Subscriptions[0].Outcome);
        Assert.Equal(RoutingOutcome.Unknown, routed.Subscriptions[1].Outcome);

        var withQueue = TopicRouting.Route("events",
            [new SubscriptionRules("orders", [new SubscriptionRule("#", RuleFilterKind.TopicBinding) { Expression = "#" }]) { Service = RoutingService.RabbitMq }, .. bindings],
            Message("order.created"), RoutingService.RabbitMq);
        Assert.Equal(RoutingOutcome.Skips, withQueue.Subscriptions.Single(item => item.Subscription == "unrouted").Outcome);
    }

    [Theory]
    [InlineData("attributes.region = \"\\u0045U\"")]
    [InlineData("attributes.note = \"a\\nb\"")]
    [InlineData("attributes.\"my\\u002Dkey\" = \"x\"")]
    public void Pub_Sub_filters_with_escapes_are_left_to_Pub_Sub(string filter)
    {
        // Google's documentation reads \u0045 as E; the Pub/Sub emulator compares the backslash literally.
        var rule = new SubscriptionRule("filter", RuleFilterKind.PubSubFilter) { Expression = filter };
        var result = TopicRouting.Check(rule, Message(null, null, ("region", "EU"), ("note", "a\nb"), ("my-key", "x")));
        Assert.Equal(RoutingOutcome.Unknown, result.Outcome);
        Assert.Contains("Pub/Sub decides", result.Explanation, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("$.id == 9007199254740993", true)]
    [InlineData("$.id == 9007199254740992", false)]
    [InlineData("$.id > 9007199254740992", true)]
    [InlineData("$.text == 9007199254740993", true)]
    [InlineData("$.text == 9007199254740992", false)]
    [InlineData("$.price == 10.10", true)]
    public void JSON_search_compares_large_numbers_exactly(string query, bool expected)
    {
        var message = new BrowsedMessage(ServiceBusEntityReference.Queue("orders"), ServiceBusSubQueue.DeadLetter, 1,
            Encoding.UTF8.GetBytes("""{"id": 9007199254740993, "text": "9007199254740993", "price": 10.1}"""), EditableMessageProperties.Empty);
        Assert.Equal(expected, MessageSearchQuery.Parse(query).Matches(message));
    }

    [Fact]
    public void A_singular_Protobuf_field_seen_twice_keeps_the_last_value_and_merges_messages()
    {
        var schemas = ProtoSchemaSet.FromProtoFiles([("t.proto", """
            syntax = "proto3";
            message Inner { int32 a = 1; int32 b = 2; repeated int32 r = 3; }
            message Test { int32 n = 1; Inner inner = 2; repeated int32 many = 3; }
            """)]);
        // n=1, n=2, inner{a=1, r=[5]}, inner{b=2, r=[6]}, many=7, many=8 (unpacked)
        byte[] body = [0x08, 0x01, 0x08, 0x02, 0x12, 0x04, 0x08, 0x01, 0x18, 0x05, 0x12, 0x04, 0x10, 0x02, 0x18, 0x06, 0x18, 0x07, 0x18, 0x08];
        var decoded = ProtoDecoder.Decode(body, schemas.Resolve("Test")!, schemas)!.Value;
        using var json = JsonDocument.Parse(decoded.Json);
        Assert.Equal(2, json.RootElement.GetProperty("n").GetInt32());
        var inner = json.RootElement.GetProperty("inner");
        Assert.Equal(1, inner.GetProperty("a").GetInt32());
        Assert.Equal(2, inner.GetProperty("b").GetInt32());
        Assert.Equal([5, 6], inner.GetProperty("r").EnumerateArray().Select(item => item.GetInt32()));
        Assert.Equal([7, 8], json.RootElement.GetProperty("many").EnumerateArray().Select(item => item.GetInt32()));
    }

    [Fact]
    public void Packed_64_bit_integers_keep_every_digit()
    {
        var schemas = ProtoSchemaSet.FromProtoFiles([("t.proto", """
            syntax = "proto3";
            message Ids { repeated fixed64 ids = 1; repeated sfixed64 signed = 2; fixed64 one = 3; }
            """)]);
        var big = BitConverter.GetBytes(9007199254740993UL);
        var negative = BitConverter.GetBytes(-9007199254740993L);
        byte[] body = [0x0A, 8, .. big, 0x12, 8, .. negative, 0x19, .. big];
        using var json = JsonDocument.Parse(ProtoDecoder.Decode(body, schemas.Resolve("Ids")!, schemas)!.Value.Json);
        Assert.Equal(9007199254740993UL, json.RootElement.GetProperty("ids")[0].GetUInt64());
        Assert.Equal(-9007199254740993L, json.RootElement.GetProperty("signed")[0].GetInt64());
        Assert.Equal(9007199254740993UL, json.RootElement.GetProperty("one").GetUInt64());
    }
}
