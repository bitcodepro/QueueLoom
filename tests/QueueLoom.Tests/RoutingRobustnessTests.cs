using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

/// <summary>Routing never crashes or hangs on unusual values, and compares numbers as the services do.</summary>
public sealed class RoutingRobustnessTests
{
    private static RoutingMessage With(string name, object? value) =>
        new(EditableMessageProperties.Empty, new Dictionary<string, object?> { [name] = value });

    [Theory]
    [InlineData("amount / -1 > 0")]
    [InlineData("amount % -1 = 0")]
    [InlineData("amount * 2 > 0")]
    [InlineData("amount - 1 < 0")]
    public void Int64_overflow_in_SQL_is_left_to_Service_Bus(string filter)
    {
        var result = TopicRouting.Check(new SubscriptionRule("r", RuleFilterKind.Sql, filter), With("amount", long.MinValue));
        Assert.Equal(RoutingOutcome.Unknown, result.Outcome);
    }

    [Fact]
    public void A_constant_overflow_never_throws()
    {
        var rule = new SubscriptionRule("r", RuleFilterKind.Sql, "(-9223372036854775807 - 1) / -1 = 0");
        Assert.Equal(RoutingOutcome.Unknown, TopicRouting.Check(rule, With("x", 1L)).Outcome);
    }

    [Fact]
    public async Task LIKE_with_many_wildcards_finishes_quickly()
    {
        var rule = new SubscriptionRule("r", RuleFilterKind.Sql, "s LIKE '%a%a%a%a%a%a%a%a%a%a%b'");
        var task = Task.Run(() => TopicRouting.Check(rule, With("s", new string('a', 60))));
        var result = await task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(RoutingOutcome.Skips, result.Outcome);
    }

    [Theory]
    [InlineData("s LIKE 'a%c'", "abc", true)]
    [InlineData("s LIKE 'a_c'", "abc", true)]
    [InlineData("s LIKE 'a_c'", "abbc", false)]
    [InlineData("s LIKE '%'", "", true)]
    [InlineData("s LIKE '50!%' ESCAPE '!'", "50%", true)]
    [InlineData("s LIKE '50!%' ESCAPE '!'", "500", false)]
    [InlineData("s LIKE 'a.c'", "abc", false)]
    [InlineData("s NOT LIKE '%x%'", "abc", true)]
    public void LIKE_still_matches_as_before(string filter, string value, bool receives)
    {
        var result = TopicRouting.Check(new SubscriptionRule("r", RuleFilterKind.Sql, filter), With("s", value));
        Assert.Equal(receives ? RoutingOutcome.Receives : RoutingOutcome.Skips, result.Outcome);
    }

    [Fact]
    public void A_Single_header_matches_the_double_QueueLoom_sends_to_RabbitMQ()
    {
        var binding = new Dictionary<string, object?> { ["x-match"] = "all", ["price"] = 0.1d };
        var message = new RoutingMessage(EditableMessageProperties.Empty, [ApplicationPropertyValues.FromObject("price", 0.1f)]);
        Assert.Equal(RoutingOutcome.Receives, RabbitBindings.MatchHeaders(binding, message).Outcome);
    }

    [Fact]
    public void A_Single_attribute_matches_its_number_text_in_SNS()
    {
        var message = new RoutingMessage(EditableMessageProperties.Empty, [ApplicationPropertyValues.FromObject("price", 0.1f)]);
        Assert.Equal(RoutingOutcome.Receives, SnsFilterPolicy.Evaluate("""{"price":[0.1]}""", false, message).Outcome);
        var typed = With("price", 0.1f);
        Assert.Equal(RoutingOutcome.Receives, SnsFilterPolicy.Evaluate("""{"price":[{"numeric":["=",0.1]}]}""", false, typed).Outcome);
    }

    [Theory]
    [InlineData("orderId IS NOT NULL", RoutingOutcome.Receives)]
    [InlineData("orderId IS NULL", RoutingOutcome.Skips)]
    [InlineData("missing IS NULL", RoutingOutcome.Receives)]
    public void IS_NULL_is_decided_for_any_property_type(string filter, RoutingOutcome expected) =>
        Assert.Equal(expected, TopicRouting.Check(new SubscriptionRule("r", RuleFilterKind.Sql, filter), With("orderId", Guid.NewGuid())).Outcome);

    [Fact]
    public void SNS_anything_but_ignore_case_with_numbers_is_rejected_and_never_throws()
    {
        const string Policy = """{"a":[{"anything-but":{"equals-ignore-case":[5]}}]}""";
        Assert.Throws<FilterPolicyException>(() => SnsFilterPolicy.Validate(Policy, false));
        Assert.Null(Record.Exception(() => SnsFilterPolicy.Evaluate(Policy, false, With("a", "x"))));
    }

    [Theory]
    [InlineData(ApplicationPropertyType.Int32, "99999999999")]
    [InlineData(ApplicationPropertyType.UInt64, "-1")]
    [InlineData(ApplicationPropertyType.Byte, "300")]
    public void An_out_of_range_value_is_kept_as_text(ApplicationPropertyType type, string text)
    {
        var message = new RoutingMessage(EditableMessageProperties.Empty, [new MessageApplicationProperty("n", type, text)]);
        Assert.Equal(text, message.UserProperties["n"]);
        Assert.Throws<FormatException>(() => ApplicationPropertyValues.ToObject(new MessageApplicationProperty("n", type, text)));
    }

    [Theory]
    [InlineData(ApplicationPropertyType.Double, "1,5")]
    [InlineData(ApplicationPropertyType.Single, "1,5")]
    public void A_comma_is_not_read_as_a_thousands_separator(ApplicationPropertyType type, string text) =>
        Assert.Throws<FormatException>(() => ApplicationPropertyValues.ToObject(new MessageApplicationProperty("p", type, text)));
}
