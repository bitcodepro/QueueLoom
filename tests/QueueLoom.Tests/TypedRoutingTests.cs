using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using QueueLoom.App.ViewModels;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests;

/// <summary>Typed correlation values and multiline properties, through Core and through the routing window.</summary>
public sealed class TypedRoutingTests
{
    private static readonly Guid TraceId = Guid.Parse("4f3c2a1b-0000-4000-8000-000000000001");
    private static readonly DateTime Due = new(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc);

    public static TheoryData<string, object, MessageApplicationProperty> SameValues => new()
    {
        { "trace", TraceId, new("trace", ApplicationPropertyType.Guid, TraceId.ToString("D")) },
        { "due", Due, new("due", ApplicationPropertyType.DateTime, "2026-10-01T12:00:00.0000000Z") },
        { "wait", TimeSpan.FromMinutes(5), new("wait", ApplicationPropertyType.TimeSpan, "00:05:00") },
        { "grade", 'A', new("grade", ApplicationPropertyType.Character, "A") },
        { "ratio", 0.1f, new("ratio", ApplicationPropertyType.Single, "0.1") },
        { "price", 19.99m, new("price", ApplicationPropertyType.Decimal, "19.99") },
        { "count", 250, new("count", ApplicationPropertyType.Int32, "250") },
        { "total", 250L, new("total", ApplicationPropertyType.Int64, "250") }
    };

    [Theory]
    [MemberData(nameof(SameValues))]
    public void CorrelationCriteria_FromServiceBus_MatchTheSameMessageValue(string name, object criterion, MessageApplicationProperty property)
    {
        // The criterion as the Azure SDK hands it back, through the same mapping QueueLoom uses.
        var filter = new CorrelationRuleFilter();
        filter.ApplicationProperties[name] = criterion;
        var rule = AzureServiceBusWorkspace.ToRule(ServiceBusModelFactory.RuleProperties("typed", filter));

        var outcome = TopicRouting.Check(rule, new RoutingMessage(EditableMessageProperties.Empty, [property])).Outcome;

        Assert.Equal(RoutingOutcome.Receives, outcome);
    }

    [Fact]
    public void DifferentTypedValues_AreSkipsAndUnreadableOnesAreUnknown()
    {
        var guidRule = new SubscriptionRule("g", RuleFilterKind.Correlation,
            Correlation: new CorrelationFilterFields { Properties = new Dictionary<string, object> { ["trace"] = TraceId } });
        Assert.Equal(RoutingOutcome.Skips, TopicRouting.Check(guidRule, new RoutingMessage(EditableMessageProperties.Empty,
            [new MessageApplicationProperty("trace", ApplicationPropertyType.Guid, Guid.Empty.ToString())])).Outcome);
        Assert.Equal(RoutingOutcome.Skips, TopicRouting.Check(guidRule, new RoutingMessage(EditableMessageProperties.Empty,
            [new MessageApplicationProperty("trace", ApplicationPropertyType.String, TraceId.ToString())])).Outcome);

        var binaryRule = new SubscriptionRule("b", RuleFilterKind.Correlation,
            Correlation: new CorrelationFilterFields { Properties = new Dictionary<string, object> { ["blob"] = new byte[] { 1, 2 } } });
        Assert.Equal(RoutingOutcome.Unknown, TopicRouting.Check(binaryRule, new RoutingMessage(EditableMessageProperties.Empty,
            [new MessageApplicationProperty("blob", ApplicationPropertyType.Binary, "AQI=")])).Outcome);

        var sqlOnGuid = new SubscriptionRule("s", RuleFilterKind.Sql, $"trace = '{TraceId}'");
        Assert.Equal(RoutingOutcome.Unknown, TopicRouting.Check(sqlOnGuid, new RoutingMessage(EditableMessageProperties.Empty,
            [new MessageApplicationProperty("trace", ApplicationPropertyType.Guid, TraceId.ToString())])).Outcome);
    }

    [Theory]
    [MemberData(nameof(SameValues))]
    public void EveryTypeSurvivesFormatAndParse(string name, object value, MessageApplicationProperty property)
    {
        _ = name;
        _ = property;
        var parsed = RoutingValue.Parse(RoutingValue.Format(value));
        Assert.True(RoutingValue.AreEqual(value, parsed), $"{RoutingValue.Format(value)} → {parsed} ({parsed.GetType().Name})");
        Assert.Equal(value.GetType(), parsed.GetType());
    }

    [Fact]
    public void TextWithLineBreaksQuotesAndBackslashes_SurvivesFormatAndParse()
    {
        const string text = "line1\nline2\r\n'quoted' C:\\temp\tend";
        var line = RoutingValue.Format(text);
        Assert.DoesNotContain('\n', line);
        Assert.Equal(text, RoutingValue.Parse(line));
        Assert.Equal(250.0, RoutingValue.Parse(RoutingValue.Format(250.0)));
    }

    [Fact]
    public async Task RoutingWindow_HandlesMultilineAndTypedProperties()
    {
        SubscriptionRules[] rules =
        [
            new("all", [new SubscriptionRule(SubscriptionRule.DefaultRuleName, RuleFilterKind.True)]),
            new("by-trace", [new SubscriptionRule("trace", RuleFilterKind.Correlation, Correlation: new CorrelationFilterFields
            {
                Properties = new Dictionary<string, object> { ["trace"] = TraceId, ["ratio"] = 0.1f }
            })])
        ];
        var draft = new MessageDraft(EditableMessageBody.Empty, new EditableMessageProperties(MessageId: "m-1"),
        [
            new MessageApplicationProperty("note", ApplicationPropertyType.String, "line1\nline2"),
            new MessageApplicationProperty("trace", ApplicationPropertyType.Guid, TraceId.ToString("D")),
            new MessageApplicationProperty("ratio", ApplicationPropertyType.Single, "0.1")
        ]);
        var services = new TopicRoutingServices(_ => Task.FromResult<IReadOnlyList<SubscriptionRules>>(rules),
            (_, _, _, _) => Task.CompletedTask, (_, _, _) => Task.CompletedTask,
            _ => Task.FromResult<SubscriptionRule?>(null), (_, _, _) => Task.FromResult(false));
        var routing = new TopicRoutingViewModel("orders", "Dev", false, string.Empty, services, draft, "Draft");

        await routing.LoadAsync();

        Assert.False(routing.HasError, routing.Error);
        Assert.Equal("2 of 2 subscriptions receive it.", routing.Headline);

        // An edited line is read back with its type: the escaped line break and the <Guid> tag both survive.
        routing.TestProperties = routing.TestProperties.Replace("line2", "line3", StringComparison.Ordinal) + Environment.NewLine + "extra = 1";
        routing.Check();
        Assert.False(routing.HasError, routing.Error);
        Assert.Equal("2 of 2 subscriptions receive it.", routing.Headline);
        Assert.Equal("line1\nline3", TopicRoutingViewModel.ParseProperties(routing.TestProperties).Single(pair => pair.Key == "note").Value);
    }
}
