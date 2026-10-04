using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

public sealed class SnsAndPubSubFilterSemanticsTests
{
    private static RoutingMessage Body(string json) =>
        new(EditableMessageProperties.Empty, Array.Empty<KeyValuePair<string, object?>>()) { Body = json };

    // AWS SNS "Key matching": exists works only on leaf nodes, and for "exists": true
    // "The key must have a non-null and non-empty value."
    [Theory]
    [InlineData("""{"store": null, "other": 1}""")]
    [InlineData("""{"store": [], "other": 1}""")]
    [InlineData("""{"store": {"name": "fans"}, "other": 1}""")]
    public void Sns_body_exists_true_needs_a_non_null_non_empty_leaf(string body)
    {
        Assert.Equal(RoutingOutcome.Skips,
            SnsFilterPolicy.Evaluate("""{"store": [{"exists": true}]}""", true, Body(body)).Outcome);
    }

    // AWS SNS "Filter policy constraints": "Amazon SNS ignores message attributes with the Binary data type."
    // QueueLoom publishes Binary application properties as SNS Binary attributes (AwsMessageMapper.ToAttribute).
    [Fact]
    public void Sns_ignores_binary_attributes_for_exists()
    {
        var message = new RoutingMessage(EditableMessageProperties.Empty,
        [
            new MessageApplicationProperty("signature", ApplicationPropertyType.Binary, Convert.ToBase64String([1, 2, 3])),
            new MessageApplicationProperty("region", ApplicationPropertyType.String, "EU")
        ]);
        Assert.Equal(RoutingOutcome.Skips,
            SnsFilterPolicy.Evaluate("""{"signature": [{"exists": true}]}""", false, message).Outcome);
        Assert.Equal(RoutingOutcome.Receives,
            SnsFilterPolicy.Evaluate("""{"signature": [{"exists": false}]}""", false, message).Outcome);
    }

    // AWS SNS "String value matching": the wildcard operator and anything-but wildcard are documented filter operators.
    [Theory]
    [InlineData("""{"customer_interests": [{"wildcard": "*ball"}]}""")]
    [InlineData("""{"customer_interests": [{"anything-but": {"wildcard": "*ball"}}]}""")]
    public void Sns_accepts_documented_wildcard_policies(string policy)
    {
        Assert.Null(Record.Exception(() => SnsFilterPolicy.Validate(policy, onBody: false)));
    }

    // Google Pub/Sub filter syntax: attribute keys need a string literal only when they contain
    // non-alphanumeric characters "besides hyphens and underscores".
    [Theory]
    [InlineData("attributes.my-key = \"x\"", true)]
    [InlineData("attributes:my-key", true)]
    [InlineData("hasPrefix(attributes.my-key, \"x\")", true)]
    public void PubSub_reads_unquoted_hyphenated_attribute_names(string filter, bool expected)
    {
        var message = new RoutingMessage(EditableMessageProperties.Empty,
            [new MessageApplicationProperty("my-key", ApplicationPropertyType.String, "x")]);
        Assert.Equal(expected, PubSubFilter.Parse(filter).Evaluate(message));
    }
}

public sealed class SnsWildcardPreviewTests
{
    // AWS SNS "String value matching": {"anything-but": {"wildcard": "*ball"}} does not match "baseball".
    [Fact]
    public void Sns_anything_but_wildcard_is_not_read_as_equals_ignore_case()
    {
        var message = new RoutingMessage(EditableMessageProperties.Empty,
            [new MessageApplicationProperty("customer_interests", ApplicationPropertyType.String, "baseball")]);
        Assert.NotEqual(RoutingOutcome.Receives,
            SnsFilterPolicy.Evaluate("""{"customer_interests": [{"anything-but": {"wildcard": "*ball"}}]}""", false, message).Outcome);
    }
}

public sealed class SnsWildcardMatchingTests
{
    [Theory]
    [InlineData("*ball", "baseball", "Receives")]
    [InlineData("*ball", "ball", "Receives")]
    [InlineData("*ball", "balloon", "Skips")]
    [InlineData("base*", "baseball", "Receives")]
    [InlineData("b*e*l", "baseball", "Receives")]
    [InlineData("b*z*l", "baseball", "Skips")]
    [InlineData("a*a", "a", "Skips")]
    [InlineData("*BALL", "baseball", "Skips")]
    public void WildcardMatchesLikeSns(string pattern, string value, string expected)
    {
        var message = new RoutingMessage(EditableMessageProperties.Empty,
            [new MessageApplicationProperty("customer_interests", ApplicationPropertyType.String, value)]);
        Assert.Equal(expected, SnsFilterPolicy.Evaluate($$"""{"customer_interests": [{"wildcard": "{{pattern}}"}]}""", false, message).Outcome.ToString());
    }

    [Fact]
    public void AMessageWithOnlyBinaryAttributesHasNoPropertiesForExistsFalse()
    {
        var message = new RoutingMessage(EditableMessageProperties.Empty,
            [new MessageApplicationProperty("signature", ApplicationPropertyType.Binary, Convert.ToBase64String([1, 2, 3]))]);
        Assert.Equal(RoutingOutcome.Skips,
            SnsFilterPolicy.Evaluate("""{"region": [{"exists": false}]}""", false, message).Outcome);
    }
}
