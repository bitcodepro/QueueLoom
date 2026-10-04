using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;

namespace QueueLoom.Tests;

public sealed class SqlShortCircuitAndHostPortTests
{
    private static readonly RoutingMessage Us = new(
        new EditableMessageProperties(MessageId: "m-1", Subject: "order.created"),
        [new MessageApplicationProperty("region", ApplicationPropertyType.String, "US")]);

    [Fact]
    public void SqlFilter_FalseAndUnsupported_IsFalse() =>
        Assert.Equal(false, SqlFilter.Parse("region = 'EU' AND sys.EnqueuedTimeUtc > 5").Evaluate(Us));

    [Fact]
    public void SqlFilter_TrueOrUnsupported_IsTrue() =>
        Assert.Equal(true, SqlFilter.Parse("region = 'US' OR sys.DeliveryCount > 1").Evaluate(Us));

    [Fact]
    public void Routing_FalseAndUnsupported_Skips()
    {
        var rule = new SubscriptionRule("eu", RuleFilterKind.Sql, "region = 'EU' AND sys.EnqueuedTimeUtc > 5");
        Assert.Equal(RoutingOutcome.Skips, TopicRouting.Check(rule, Us).Outcome);
    }

    [Theory]
    [InlineData("broker:99999")]
    [InlineData("broker:0")]
    public void Kafka_BootstrapPortOutOfRange_IsRejected(string servers)
    {
        var profile = ServiceBusProfile.CreateNew("Events", EnvironmentKind.Development, new AuthenticationSettings(AuthenticationKind.KafkaNone)) with
        { Provider = MessagingProvider.Kafka, Kafka = new KafkaSettings(servers) };
        Assert.Contains(ProfileValidator.Validate(profile).Errors, e => e.Code == "profile.kafka.servers.invalid");
    }
}
