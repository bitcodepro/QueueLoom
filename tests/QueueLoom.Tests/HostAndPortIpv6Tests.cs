using QueueLoom.Core.Profiles;
using QueueLoom.Core.Validation;

namespace QueueLoom.Tests;

// Kafka bootstrap servers and the Pub/Sub emulator address are host:port. An IPv6 address there is written in
// brackets ([::1]:9092), as librdkafka and gRPC expect it; the editor refused it although RabbitMQ accepts IPv6 hosts.
// Without brackets the port cannot be told from the address (::1:9092), so that form stays refused, with a message
// that says how to write it.
public sealed class HostAndPortIpv6Tests
{
    private static ServiceBusProfile Kafka(string servers) =>
        ServiceBusProfile.CreateNew("Kafka", EnvironmentKind.Development, new AuthenticationSettings(AuthenticationKind.KafkaNone)) with
        {
            Provider = MessagingProvider.Kafka, FullyQualifiedNamespace = null,
            Kafka = new KafkaSettings(servers)
        };

    private static ServiceBusProfile PubSub(string emulator) =>
        ServiceBusProfile.CreateNew("PubSub", EnvironmentKind.Development, new AuthenticationSettings(AuthenticationKind.GoogleApplicationDefault)) with
        {
            Provider = MessagingProvider.GooglePubSub, FullyQualifiedNamespace = null,
            GooglePubSub = new GooglePubSubSettings("orders-dev-4821", emulator)
        };

    private static IReadOnlyList<string> Codes(ServiceBusProfile profile) =>
        ProfileValidator.Validate(profile).Errors.Select(error => error.Code).ToArray();

    [Theory]
    [InlineData("[::1]:9092")]
    [InlineData("[2001:db8::5]:9093")]
    [InlineData("[fe80::1]:9092,broker-2:9092")]
    [InlineData("broker-1:9092, [::1]:9093")]
    [InlineData("localhost:9092")]
    [InlineData("10.0.0.5:9092")]
    public void KafkaAcceptsNamesIpv4AndBracketedIpv6(string servers) =>
        Assert.DoesNotContain("profile.kafka.servers.invalid", Codes(Kafka(servers)));

    [Theory]
    [InlineData("::1:9092")]
    [InlineData("[::1]")]
    [InlineData("[::1]:")]
    [InlineData("[::1]:0")]
    [InlineData("[::1]:70000")]
    [InlineData("[broker]:9092")]
    [InlineData("[10.0.0.5]:9092")]
    [InlineData("[::1]9092")]
    [InlineData("[[::1]]:9092")]
    [InlineData("[::1]]:9092")]
    [InlineData("[[::1]:9092")]
    [InlineData("broker-1")]
    public void KafkaRefusesAmbiguousOrIncompleteAddresses(string servers)
    {
        var error = Assert.Single(ProfileValidator.Validate(Kafka(servers)).Errors, error => error.Code == "profile.kafka.servers.invalid");
        Assert.Contains("[::1]:9092", error.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("[::1]:8085", true)]
    [InlineData("localhost:8085", true)]
    [InlineData("::1:8085", false)]
    [InlineData("[::1]", false)]
    [InlineData("[[::1]]:8085", false)]
    public void ThePubSubEmulatorAddressFollowsTheSameRules(string emulator, bool valid)
    {
        var codes = Codes(PubSub(emulator));
        Assert.Equal(!valid, codes.Contains("profile.gcp.emulator.invalid"));
        if (!valid)
        {
            Assert.Contains("[::1]:8085", ProfileValidator.Validate(PubSub(emulator)).Errors
                .Single(error => error.Code == "profile.gcp.emulator.invalid").Message, StringComparison.Ordinal);
        }
    }
}
