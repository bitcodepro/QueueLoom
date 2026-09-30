using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class SqlFilterTests
{
    private static readonly RoutingMessage Order = new(
        new EditableMessageProperties(MessageId: "m-1", CorrelationId: "c-9", Subject: "order.created", ContentType: "application/json"),
        [
            new MessageApplicationProperty("region", ApplicationPropertyType.String, "EU"),
            new MessageApplicationProperty("amount", ApplicationPropertyType.Int32, "250"),
            new MessageApplicationProperty("rate", ApplicationPropertyType.Double, "0.5"),
            new MessageApplicationProperty("vip", ApplicationPropertyType.Boolean, "true"),
            new MessageApplicationProperty("customer name", ApplicationPropertyType.String, "O'Brien")
        ]);

    [Theory]
    [InlineData("1=1", true)]
    [InlineData("1=0", false)]
    [InlineData("region = 'EU' AND amount > 100", true)]
    [InlineData("region = 'eu'", false)]
    [InlineData("region <> 'US' OR missing = 1", true)]
    [InlineData("NOT (region IN ('US', 'CA'))", true)]
    [InlineData("region NOT IN ('EU')", false)]
    [InlineData("sys.Label LIKE 'order.%'", true)]
    [InlineData("sys.Label LIKE 'order._reated'", true)]
    [InlineData("sys.Label NOT LIKE 'order%'", false)]
    [InlineData("sys.Label LIKE 'order!%' ESCAPE '!'", false)]
    [InlineData("user.amount * 2 >= 500 AND rate < 1", true)]
    [InlineData("amount + 0.5 = 250.5", true)]
    [InlineData("vip = TRUE", true)]
    [InlineData("missing IS NULL", true)]
    [InlineData("region IS NOT NULL", true)]
    [InlineData("EXISTS(region) AND NOT EXISTS(priority)", true)]
    [InlineData("[customer name] = 'O''Brien'", true)]
    [InlineData("sys.CorrelationId = 'c-9' AND sys.ContentType = 'application/json'", true)]
    [InlineData("-amount < 0", true)]
    public void Conditions_EvaluateLikeServiceBus(string filter, bool expected) =>
        Assert.Equal(expected, SqlFilter.Parse(filter).Evaluate(Order) == true);

    [Fact]
    public void MissingProperties_AreUnknownAndNeverMatch()
    {
        Assert.Null(SqlFilter.Parse("priority = 'high'").Evaluate(Order));
        Assert.Null(SqlFilter.Parse("NOT (priority = 'high')").Evaluate(Order));
        Assert.Equal(false, SqlFilter.Parse("priority = 'high' AND region = 'US'").Evaluate(Order));
        Assert.Equal(true, SqlFilter.Parse("priority = 'high' OR region = 'EU'").Evaluate(Order));
    }

    [Fact]
    public void Steps_ExplainWhatDidNotMatch()
    {
        var steps = new List<SqlFilterStep>();
        SqlFilter.Parse("region = 'eu' AND amount > 100").Evaluate(Order, steps);

        var failed = Assert.Single(steps, step => step.Result != true);
        Assert.Equal("region = 'eu'", failed.Text);
        Assert.Equal("region is 'EU'", failed.Actual);
    }

    [Theory]
    [InlineData("region = = 'x'")]
    [InlineData("region = 'EU")]
    [InlineData("(region = 'EU'")]
    [InlineData("")]
    [InlineData("region LIKE 5")]
    public void BadSyntax_IsReported(string filter) =>
        Assert.Throws<SqlFilterSyntaxException>(() => SqlFilter.Parse(filter));

    [Fact]
    public void WhatOnlyServiceBusKnows_IsNotGuessed()
    {
        Assert.Throws<SqlFilterNotSupportedException>(() => SqlFilter.Parse("sys.EnqueuedTimeUtc > '2026-01-01'").Evaluate(Order));
        Assert.Throws<SqlFilterNotSupportedException>(() => SqlFilter.Parse("newid() = 'x'"));
    }

    [Fact]
    public void Routing_ExplainsEverySubscription()
    {
        SubscriptionRules[] subscriptions =
        [
            new("billing", [new SubscriptionRule("eu", RuleFilterKind.Sql, "region = 'EU' AND amount > 100")]),
            new("audit", [new SubscriptionRule(SubscriptionRule.DefaultRuleName, RuleFilterKind.True)]),
            new("shipping", [new SubscriptionRule("created", RuleFilterKind.Correlation, Correlation: new CorrelationFilterFields(Subject: "order.shipped"))]),
            new("orphan", []),
            new("timed", [new SubscriptionRule("late", RuleFilterKind.Sql, "sys.EnqueuedTimeUtc > '2026-01-01'")])
        ];

        var result = TopicRouting.Route("orders", subscriptions, Order);

        Assert.Equal(RoutingOutcome.Receives, result.Subscriptions.Single(item => item.Subscription == "billing").Outcome);
        Assert.Equal("Receives it through rule $Default", result.Subscriptions.Single(item => item.Subscription == "audit").Summary);
        var shipping = result.Subscriptions.Single(item => item.Subscription == "shipping");
        Assert.Equal(RoutingOutcome.Skips, shipping.Outcome);
        Assert.Contains("Subject should be 'order.shipped' but is 'order.created'", shipping.Summary, StringComparison.Ordinal);
        Assert.Equal("Has no rules, so it receives no messages at all.", result.Subscriptions.Single(item => item.Subscription == "orphan").Summary);
        Assert.Equal(RoutingOutcome.Unknown, result.Subscriptions.Single(item => item.Subscription == "timed").Outcome);
        Assert.Equal("2 of 5 subscriptions receive it; 1 depends on what only Service Bus knows.", result.Headline);

        var dropped = TopicRouting.Route("orders", [subscriptions[2], subscriptions[3]], Order);
        Assert.True(dropped.IsDropped);
        Assert.StartsWith("No subscription takes this message", dropped.Headline, StringComparison.Ordinal);
    }
}
