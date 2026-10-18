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

    // The alternatives of an $or add up; a body leaf counts its values times its nesting depth (10x2 by 16x2).
    [Theory]
    [InlineData("or", false, 160)]
    [InlineData("nested", true, 640)]
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

    // A body leaf at depth two counts double: 75 values are 150 cases, 76 are 152.
    [Theory]
    [InlineData(75, true)]
    [InlineData(76, false)]
    public void ABodyLeafCountsItsNestingDepth(int values, bool accepted)
    {
        var policy = new JsonObject { ["order"] = new JsonObject { ["region"] = Values(values) } }.ToJsonString();

        if (accepted)
        {
            SnsFilterPolicy.Validate(policy, onBody: true);
        }
        else
        {
            Assert.Contains("152 cases", Assert.Throws<FilterPolicyException>(() => SnsFilterPolicy.Validate(policy, onBody: true)).Message, StringComparison.Ordinal);
        }
    }

    // $or branches keep the depth they sit at: inside "order" each branch's leaf counts double (40x2 + 40x2 = 160).
    [Fact]
    public void OrBranchesKeepTheirNestingDepth()
    {
        var policy = new JsonObject
        {
            ["order"] = new JsonObject { ["$or"] = new JsonArray(new JsonObject { ["a"] = Values(40) }, new JsonObject { ["b"] = Values(40) }) }
        }.ToJsonString();

        Assert.Contains("160 cases", Assert.Throws<FilterPolicyException>(() => SnsFilterPolicy.Validate(policy, onBody: true)).Message, StringComparison.Ordinal);
    }

    // AWS reads a $or whose objects hold operators (numeric ranges here) as an ordinary field: 5 x 4 x 2 = 40, accepted.
    [Fact]
    public void AnOrOfOperatorObjectsCountsAsAnOrdinaryField()
    {
        const string policy = """{"gate":[1,2,3,4,5],"stage":[1,2,3,4],"$or":[{"numeric":[">",0,"<",10]},{"numeric":[">",20,"<",30]}]}""";

        Assert.Equal(40, SnsFilterPolicy.Combinations(System.Text.Json.JsonDocument.Parse(policy).RootElement));
        SnsFilterPolicy.Validate(policy, onBody: true);
    }
}
