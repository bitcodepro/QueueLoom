using System.Text.Json.Nodes;
using QueueLoom.Core.Routing;

namespace QueueLoom.Tests;

// SNS refuses a filter policy whose values combine into more than 150 cases. QueueLoom accepted such a policy in the
// rule editor and found out only when SetSubscriptionAttributes failed; it now refuses it first, saying why.
public sealed class SnsFilterCombinationTests
{
    private static JsonArray Values(int count) => new(Enumerable.Range(0, count).Select(index => (JsonNode)JsonValue.Create($"v{index}")).ToArray());

    private static string Policy(params int[] valuesPerKey)
    {
        var policy = new JsonObject();
        for (var key = 0; key < valuesPerKey.Length; key++) policy[$"k{key}"] = Values(valuesPerKey[key]);
        return policy.ToJsonString();
    }

    [Theory]
    [InlineData(new[] { 5, 5, 6 }, true)]   // 150
    [InlineData(new[] { 150 }, true)]
    [InlineData(new[] { 151 }, false)]
    [InlineData(new[] { 5, 5, 7 }, false)]  // 175
    public void AttributePoliciesAreLimitedTo150Combinations(int[] valuesPerKey, bool accepted)
    {
        var validate = () => SnsFilterPolicy.Validate(Policy(valuesPerKey), onBody: false);

        if (accepted)
        {
            validate();
        }
        else
        {
            var error = Assert.Throws<FilterPolicyException>(validate);
            Assert.Contains("at most 150", error.Message, StringComparison.Ordinal);
        }
    }

    // The alternatives of an $or add up; nested keys of a body policy multiply.
    [Theory]
    [InlineData("or", false, 160)]
    [InlineData("nested", true, 160)]
    public void OrAndNestedKeysCountAsSnsCountsThem(string shape, bool onBody, long expected)
    {
        var policy = shape == "or"
            ? new JsonObject { ["$or"] = new JsonArray(new JsonObject { ["a"] = Values(100) }, new JsonObject { ["b"] = Values(60) }) }
            : new JsonObject { ["order"] = new JsonObject { ["region"] = Values(10), ["status"] = Values(16) } };

        var error = Assert.Throws<FilterPolicyException>(() => SnsFilterPolicy.Validate(policy.ToJsonString(), onBody));

        Assert.Contains($"{expected:N0} cases", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnEnormousProductIsReportedWithoutOverflowing()
    {
        var error = Assert.Throws<FilterPolicyException>(() => SnsFilterPolicy.Validate(Policy(Enumerable.Repeat(100, 12).ToArray()), onBody: false));

        Assert.Contains("too many cases", error.Message, StringComparison.Ordinal);
    }
}
