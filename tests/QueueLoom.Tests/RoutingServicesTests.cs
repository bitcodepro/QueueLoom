using QueueLoom.App.ViewModels;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class SnsFilterPolicyEvaluationTests
{
    private static RoutingMessage Message(params (string Name, object Value)[] attributes) =>
        new(new EditableMessageProperties(Subject: "order.created"), attributes.Select(pair => new KeyValuePair<string, object?>(pair.Name, pair.Value)));

    private static RoutingOutcome Check(string policy, RoutingMessage message, bool onBody = false) =>
        SnsFilterPolicy.Evaluate(policy, onBody, message).Outcome;

    [Theory]
    [InlineData("""{"region": ["EU"]}""", RoutingOutcome.Receives)]
    [InlineData("""{"region": ["eu"]}""", RoutingOutcome.Skips)]
    [InlineData("""{"region": [{"equals-ignore-case": "eu"}]}""", RoutingOutcome.Receives)]
    [InlineData("""{"region": [{"prefix": "E"}]}""", RoutingOutcome.Receives)]
    [InlineData("""{"region": [{"suffix": "U"}]}""", RoutingOutcome.Receives)]
    [InlineData("""{"region": [{"anything-but": ["EU", "US"]}]}""", RoutingOutcome.Skips)]
    [InlineData("""{"region": [{"anything-but": {"prefix": "U"}}]}""", RoutingOutcome.Receives)]
    [InlineData("""{"amount": [{"numeric": [">", 100, "<=", 500]}]}""", RoutingOutcome.Receives)]
    [InlineData("""{"amount": [{"numeric": ["<", 100]}]}""", RoutingOutcome.Skips)]
    [InlineData("""{"amount": [250]}""", RoutingOutcome.Receives)]
    [InlineData("""{"region": [{"exists": true}], "tier": [{"exists": false}]}""", RoutingOutcome.Receives)]
    [InlineData("""{"Region": ["EU"]}""", RoutingOutcome.Skips)]
    [InlineData("""{"Subject": [{"prefix": "order."}]}""", RoutingOutcome.Receives)]
    [InlineData("""{"$or": [{"region": ["US"]}, {"amount": [{"numeric": [">=", 250]}]}]}""", RoutingOutcome.Receives)]
    [InlineData("""{"ip": [{"cidr": "10.0.0.0/24"}]}""", RoutingOutcome.Receives)]
    public void Attribute_policies_match_as_SNS_does(string policy, RoutingOutcome expected) =>
        Assert.Equal(expected, Check(policy, Message(("region", "EU"), ("amount", 250L), ("ip", "10.0.0.7"))));

    [Fact]
    public void A_missing_attribute_matches_nothing_but_exists_false()
    {
        var message = Message(("tier", "gold"));
        Assert.Equal(RoutingOutcome.Skips, Check("""{"region": [{"anything-but": "EU"}]}""", message));
        Assert.Equal(RoutingOutcome.Receives, Check("""{"region": [{"exists": false}]}""", message));
        var (_, explanation) = SnsFilterPolicy.Evaluate("""{"region": ["EU"]}""", false, message);
        Assert.Equal("""region is missing; the policy wants ["EU"]""", explanation);
    }

    [Fact]
    public void Comparing_text_with_numbers_is_left_to_SNS()
    {
        var (outcome, explanation) = SnsFilterPolicy.Evaluate("""{"amount": ["250"]}""", false, Message(("amount", 250L)));
        Assert.Equal(RoutingOutcome.Unknown, outcome);
        Assert.Contains("SNS decides", explanation, StringComparison.Ordinal);
    }

    [Fact]
    public void Body_policies_look_into_nested_JSON_and_arrays()
    {
        var message = new RoutingMessage(EditableMessageProperties.Empty, Array.Empty<KeyValuePair<string, object?>>())
        {
            Body = """{"order": {"status": "failed", "tags": ["eu", "vip"], "total": 99.5, "paid": false}}"""
        };
        Assert.Equal(RoutingOutcome.Receives, Check("""{"order": {"status": ["failed"], "tags": ["vip"]}}""", message, onBody: true));
        Assert.Equal(RoutingOutcome.Receives, Check("""{"order": {"paid": [false], "total": [{"numeric": ["<", 100]}]}}""", message, onBody: true));
        Assert.Equal(RoutingOutcome.Skips, Check("""{"order": {"status": ["shipped"]}}""", message, onBody: true));
        Assert.Equal(RoutingOutcome.Skips, Check("""{"order": ["failed"]}""", message, onBody: false));
        Assert.Equal(RoutingOutcome.Skips,
            Check("""{"order": ["x"]}""", new RoutingMessage(EditableMessageProperties.Empty, Array.Empty<KeyValuePair<string, object?>>()) { Body = "not json" }, onBody: true));
    }

    [Theory]
    [InlineData("""["EU"]""", "JSON object")]
    [InlineData("""{"region": "EU"}""", "list of values")]
    [InlineData("""{"region": [{"glob": "E*"}]}""", "no operator")]
    [InlineData("""{"amount": [{"numeric": ["<", 5, ">", 1]}]}""", "does not take")]
    [InlineData("""{"order": {"status": ["x"]}}""", "MessageBody")]
    [InlineData("""{"region": [""", "not valid JSON")]
    public void Policies_SNS_would_reject_are_explained(string policy, string reason)
    {
        var exception = Assert.Throws<FilterPolicyException>(() => SnsFilterPolicy.Validate(policy, onBody: false));
        Assert.Contains(reason, exception.Message, StringComparison.Ordinal);
    }
}

public sealed class PubSubFilterEvaluationTests
{
    private static readonly RoutingMessage Message = new(new EditableMessageProperties(Subject: "order.created"),
        [new MessageApplicationProperty("region", ApplicationPropertyType.String, "EU"), new MessageApplicationProperty("my-key", ApplicationPropertyType.String, "x"),
            new MessageApplicationProperty("amount", ApplicationPropertyType.Int64, "250")]);

    [Theory]
    [InlineData("attributes.region = \"EU\"", true)]
    [InlineData("attributes.region = \"eu\"", false)]
    [InlineData("attributes.Region = \"EU\"", false)]
    [InlineData("attributes.region != \"US\"", true)]
    [InlineData("attributes.tier != \"gold\"", true)]
    [InlineData("attributes:region", true)]
    [InlineData("NOT attributes:tier", true)]
    [InlineData("-attributes:region", false)]
    [InlineData("hasPrefix(attributes.Subject, \"order.\")", true)]
    [InlineData("hasPrefix(attributes.tier, \"g\")", false)]
    [InlineData("attributes.\"my-key\" = \"x\"", true)]
    [InlineData("attributes.amount = \"250\"", true)]
    [InlineData("attributes.region = \"EU\" AND (attributes:tier OR attributes.amount = \"250\")", true)]
    [InlineData("attributes.region = \"US\" OR attributes.region = \"CA\"", false)]
    public void Filters_match_as_Pub_Sub_does(string filter, bool expected) =>
        Assert.Equal(expected, PubSubFilter.Parse(filter).Evaluate(Message));

    [Theory]
    [InlineData("attributes.a = \"1\" AND attributes.b = \"2\" OR attributes.c = \"3\"", "parentheses")]
    [InlineData("attributes.a = 1", "quoted")]
    [InlineData("region = \"EU\"", "Expected attributes")]
    [InlineData("(attributes:a", "')'")]
    public void Filters_Pub_Sub_would_reject_are_explained(string filter, string reason)
    {
        var exception = Assert.Throws<SqlFilterSyntaxException>(() => PubSubFilter.Parse(filter));
        Assert.Contains(reason, exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_failed_comparison_names_the_value_it_saw()
    {
        var rule = new SubscriptionRule("filter", RuleFilterKind.PubSubFilter) { Expression = "attributes.region = \"US\"" };
        var result = TopicRouting.Check(rule, Message);
        Assert.Equal(RoutingOutcome.Skips, result.Outcome);
        Assert.Equal("attributes.region = \"US\" is false (region is \"EU\")", result.Explanation);
    }
}

public sealed class RabbitBindingEvaluationTests
{
    private static RoutingMessage Message(string? routingKey, params (string Name, object Value)[] headers) =>
        new(new EditableMessageProperties(Subject: routingKey), headers.Select(pair => new KeyValuePair<string, object?>(pair.Name, pair.Value)));

    [Theory]
    [InlineData("order.*", "order.created", true)]
    [InlineData("order.*", "order.eu.created", false)]
    [InlineData("order.#", "order", true)]
    [InlineData("#.created", "order.eu.created", true)]
    [InlineData("*.eu", "invoice.eu", true)]
    [InlineData("#", "", true)]
    public void Topic_bindings_match_word_by_word(string pattern, string key, bool expected) =>
        Assert.Equal(expected, RabbitBindings.TopicMatches(pattern, key));

    [Fact]
    public void Direct_bindings_compare_the_routing_key_exactly()
    {
        var rule = new SubscriptionRule("orders", RuleFilterKind.DirectBinding) { Expression = "orders" };
        Assert.Equal(RoutingOutcome.Receives, TopicRouting.Check(rule, Message("orders")).Outcome);
        var skipped = TopicRouting.Check(rule, Message("Orders"));
        Assert.Equal(RoutingOutcome.Skips, skipped.Outcome);
        Assert.Equal("The routing key 'Orders' is not 'orders'", skipped.Explanation);
        Assert.Contains("no routing key", TopicRouting.Check(rule, Message(null)).Explanation, StringComparison.Ordinal);
    }

    [Fact]
    public void Headers_bindings_compare_types_and_names_exactly()
    {
        var all = new Dictionary<string, object?> { ["x-match"] = "all", ["region"] = "EU", ["amount"] = 250L };
        Assert.Equal(RoutingOutcome.Receives, RabbitBindings.MatchHeaders(all, Message(null, ("region", "EU"), ("amount", 250))).Outcome);
        Assert.Equal(RoutingOutcome.Skips, RabbitBindings.MatchHeaders(all, Message(null, ("region", "EU"), ("amount", "250"))).Outcome);
        Assert.Equal(RoutingOutcome.Skips, RabbitBindings.MatchHeaders(all, Message(null, ("region", "EU"), ("amount", 250.0))).Outcome);
        var (outcome, explanation) = RabbitBindings.MatchHeaders(all, Message(null, ("Region", "EU"), ("amount", 250L)));
        Assert.Equal(RoutingOutcome.Skips, outcome);
        Assert.Equal("header region is missing", explanation);

        var any = new Dictionary<string, object?> { ["x-match"] = "any", ["tier"] = "gold", ["vip"] = true, ["x-tenant"] = "acme" };
        Assert.Equal(RoutingOutcome.Receives, RabbitBindings.MatchHeaders(any, Message(null, ("vip", true))).Outcome);
        Assert.Equal(RoutingOutcome.Skips, RabbitBindings.MatchHeaders(any, Message(null, ("x-tenant", "acme"))).Outcome);
        var withX = new Dictionary<string, object?> { ["x-match"] = "any-with-x", ["x-tenant"] = "acme" };
        Assert.Equal(RoutingOutcome.Receives, RabbitBindings.MatchHeaders(withX, Message(null, ("x-tenant", "acme"))).Outcome);
    }

    [Fact]
    public void The_alternate_exchange_gets_only_what_no_binding_takes()
    {
        SubscriptionRules[] bindings =
        [
            new("orders", [new SubscriptionRule("order.*", RuleFilterKind.TopicBinding) { Expression = "order.*" }]) { Service = RoutingService.RabbitMq },
            new("unrouted", []) { Service = RoutingService.RabbitMq, IsFallback = true, IsExchange = true }
        ];
        var routed = TopicRouting.Route("events", bindings, Message("order.created"), RoutingService.RabbitMq);
        Assert.Equal([RoutingOutcome.Receives, RoutingOutcome.Skips], routed.Subscriptions.Select(item => item.Outcome));
        var unrouted = TopicRouting.Route("events", bindings, Message("invoice.sent"), RoutingService.RabbitMq);
        Assert.Equal([RoutingOutcome.Skips, RoutingOutcome.Receives], unrouted.Subscriptions.Select(item => item.Outcome));
        Assert.False(unrouted.IsDropped);
        Assert.Equal("1 of 2 destinations receive it.", unrouted.Headline);
        Assert.Null(bindings[1].Warning);
    }

    [Fact]
    public void A_message_no_binding_takes_is_dropped()
    {
        SubscriptionRules[] bindings =
        [
            new("orders", [new SubscriptionRule("orders", RuleFilterKind.DirectBinding) { Expression = "orders" }]) { Service = RoutingService.RabbitMq }
        ];
        var routed = TopicRouting.Route("direct", bindings, Message("invoices"), RoutingService.RabbitMq);
        Assert.True(routed.IsDropped);
        Assert.StartsWith("No destination takes this message. RabbitMQ drops it", routed.Headline, StringComparison.Ordinal);
        Assert.Equal("Exchange empty has no bindings: RabbitMQ drops it, or returns it to a publisher that set the mandatory flag.",
            TopicRouting.Route("empty", [], Message("x"), RoutingService.RabbitMq).Headline);
    }
}

public sealed class RoutingAcrossServicesTests
{
    [Fact]
    public void A_subscription_without_a_filter_receives_everything_in_SNS_and_Pub_Sub_but_nothing_in_Service_Bus()
    {
        var message = new RoutingMessage(EditableMessageProperties.Empty, Array.Empty<KeyValuePair<string, object?>>());
        foreach (var service in new[] { RoutingService.Sns, RoutingService.PubSub })
        {
            var routing = TopicRouting.Route("events", [new SubscriptionRules("audit", []) { Service = service }], message, service);
            Assert.Equal(RoutingOutcome.Receives, routing.Subscriptions.Single().Outcome);
            Assert.Equal("Has no filter, so it receives every message.", routing.Subscriptions.Single().Summary);
        }
        var serviceBus = TopicRouting.Route("events", [new SubscriptionRules("audit", [])], message);
        Assert.Equal(RoutingOutcome.Skips, serviceBus.Subscriptions.Single().Outcome);
    }

    [Fact]
    public void A_subscription_with_a_problem_receives_nothing_whatever_its_filter()
    {
        var rules = new SubscriptionRules("billing", []) { Service = RoutingService.Sns, Problem = "Pending confirmation." };
        var routing = TopicRouting.Route("events", [rules],
            new RoutingMessage(EditableMessageProperties.Empty, Array.Empty<KeyValuePair<string, object?>>()), RoutingService.Sns);
        Assert.Equal(RoutingOutcome.Skips, routing.Subscriptions.Single().Outcome);
        Assert.Equal("Pending confirmation.", routing.Subscriptions.Single().Summary);
        Assert.Equal("No subscription takes this message. SNS accepts it and drops it without an error.", routing.Headline);
    }

    [Fact]
    public void Pub_Sub_attributes_carry_the_text_a_property_was_sent_with()
    {
        var message = new RoutingMessage(EditableMessageProperties.Empty, [new MessageApplicationProperty("vip", ApplicationPropertyType.Boolean, "True")]);
        Assert.True(PubSubFilter.Parse("attributes.vip = \"True\"").Evaluate(message));
        var typed = new RoutingMessage(EditableMessageProperties.Empty, [new KeyValuePair<string, object?>("vip", true)]);
        Assert.True(PubSubFilter.Parse("attributes.vip = \"true\"").Evaluate(typed));
    }
}

public sealed class RuleEditorForOtherServicesTests
{
    [Fact]
    public void The_SNS_editor_checks_the_policy_and_keeps_its_scope()
    {
        var editor = new RuleEditorViewModel("events", "sqs:billing", service: RoutingService.Sns) { Policy = """{"region": "EU"}""" };
        Assert.True(editor.IsSnsPolicy);
        Assert.False(editor.IsSql);
        Assert.True(editor.HasPolicyProblem);
        Assert.Null(editor.TryBuild());
        Assert.Contains("list of values", editor.Error, StringComparison.Ordinal);

        editor.Policy = """{"order": {"region": ["EU"]}}""";
        editor.PolicyScope = RuleEditorViewModel.PolicyScopes[1];
        var rule = editor.TryBuild()!;
        Assert.Equal(RuleFilterKind.SnsFilterPolicy, rule.Kind);
        Assert.True(rule.OnMessageBody);
        Assert.Equal("""{"order":{"region":["EU"]}}""", rule.Expression);

        var existing = new RuleEditorViewModel("events", "sqs:billing", rule, RoutingService.Sns);
        Assert.Same(RuleEditorViewModel.PolicyScopes[1], existing.PolicyScope);
        Assert.Contains("\"region\"", existing.Policy, StringComparison.Ordinal);
    }

    [Fact]
    public void The_RabbitMQ_editor_builds_key_and_headers_bindings()
    {
        var topic = new RuleEditorViewModel("events", "orders", service: RoutingService.RabbitMq, bindingKind: RuleFilterKind.TopicBinding)
        {
            BindingKey = "order.#"
        };
        Assert.True(topic.IsBindingKey);
        Assert.Equal("BINDING PATTERN", topic.BindingKeyLabel);
        Assert.Equal("order.#", topic.TryBuild()!.Expression);

        var headers = new RuleEditorViewModel("by-header", "gold", service: RoutingService.RabbitMq, bindingKind: RuleFilterKind.HeadersBinding)
        {
            HeadersMatch = RuleEditorViewModel.HeaderModes[1]
        };
        Assert.Null(headers.TryBuild());
        Assert.Contains("at least one header", headers.Error, StringComparison.Ordinal);
        headers.Headers = "region = 'EU'\namount = <Int32> 250\nvip = true";
        var binding = headers.TryBuild()!;
        Assert.Equal("any", binding.Arguments["x-match"]);
        Assert.Equal(250L, binding.Arguments["amount"]);
        Assert.Equal(true, binding.Arguments["vip"]);
        headers.Headers = "trace = <Guid> 4f3c2a1b-0000-4000-8000-000000000001";
        Assert.Null(headers.TryBuild());
        Assert.Contains("not a Guid", headers.Error, StringComparison.Ordinal);

        var edit = new RuleEditorViewModel("by-header", "gold", binding, RoutingService.RabbitMq);
        Assert.Same(RuleEditorViewModel.HeaderModes[1], edit.HeadersMatch);
        Assert.Contains("amount = 250", edit.Headers, StringComparison.Ordinal);
    }
}
