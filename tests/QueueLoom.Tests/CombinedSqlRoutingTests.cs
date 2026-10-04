using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class CombinedSqlRoutingTests
{
    private static string Check(string filter, object value) =>
        TopicRouting.Check(new SubscriptionRule("r", RuleFilterKind.Sql, filter),
            new RoutingMessage(EditableMessageProperties.Empty, new Dictionary<string, object?> { ["amount"] = value, ["region"] = "US" })).Outcome.ToString();

    [Theory]
    [InlineData("amount > 2.5", "Receives")]
    [InlineData("amount = 5.0", "Receives")]
    [InlineData("amount IN (5.0, 6.0)", "Receives")]
    [InlineData("amount * 1.5 > 7", "Receives")]
    [InlineData("amount / 2 > 2", "Receives")]
    [InlineData("region = 'EU' AND amount > 2.5", "Skips")]
    [InlineData("region = 'US' OR amount > 2.5", "Receives")]
    public void WholeDecimal(string filter, string expected) => Assert.Equal(expected, Check(filter, 5m));

    [Theory]
    [InlineData("region = 'EU' AND amount > 1", "Skips")]
    [InlineData("region = 'US' OR amount > 1", "Receives")]
    [InlineData("region = 'US' AND amount > 1", "Unknown")]
    public void InexactDecimalWithDecisiveSide(string filter, string expected) => Assert.Equal(expected, Check(filter, 1.2345678901234567m));
}
