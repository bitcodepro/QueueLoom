using QueueLoom.Core.Profiles;
using QueueLoom.Core.Validation;

namespace QueueLoom.Tests;

// Docker Compose service names often hold underscores (kafka_1, pubsub_emulator, rabbit_mq), and Docker's DNS, librdkafka,
// gRPC and the RabbitMQ client all resolve them. The editor refused such hosts for Kafka bootstrap servers, the Pub/Sub
// emulator and RabbitMQ. Underscores are now accepted in host names; spaces, schemes and ports in the RabbitMQ host stay
// refused.
public sealed class HostNameUnderscoreTests
{
    private static ServiceBusProfile Kafka(string servers) =>
        ServiceBusProfile.CreateNew("Kafka", EnvironmentKind.Development, new AuthenticationSettings(AuthenticationKind.KafkaNone)) with
        {
            Provider = MessagingProvider.Kafka, FullyQualifiedNamespace = null, Kafka = new KafkaSettings(servers)
        };

    private static ServiceBusProfile PubSub(string emulator) =>
        ServiceBusProfile.CreateNew("PubSub", EnvironmentKind.Development, new AuthenticationSettings(AuthenticationKind.GoogleApplicationDefault)) with
        {
            Provider = MessagingProvider.GooglePubSub, FullyQualifiedNamespace = null, GooglePubSub = new GooglePubSubSettings("orders-dev-4821", emulator)
        };

    private static ServiceBusProfile Rabbit(string host) =>
        ServiceBusProfile.CreateNew("Rabbit", EnvironmentKind.Development, new AuthenticationSettings(AuthenticationKind.RabbitMqPassword)) with
        {
            Provider = MessagingProvider.RabbitMq, FullyQualifiedNamespace = null, RabbitMq = new RabbitMqSettings(host, "guest", "/", 5672, 15672)
        };

    private static IReadOnlyList<string> Codes(ServiceBusProfile profile) =>
        ProfileValidator.Validate(profile).Errors.Select(error => error.Code).ToArray();

    [Theory]
    [InlineData("kafka_1:9092")]
    [InlineData("kafka_1:9092,kafka_2:9093")]
    [InlineData("my_stack-kafka.internal:9092")]
    public void KafkaAcceptsComposeServiceNames(string servers) =>
        Assert.DoesNotContain("profile.kafka.servers.invalid", Codes(Kafka(servers)));

    [Theory]
    [InlineData("kafka 1:9092")]
    [InlineData("kafka_1")]
    [InlineData("kafka_1:0")]
    public void KafkaStillRefusesWhatIsNotHostAndPort(string servers) =>
        Assert.Contains("profile.kafka.servers.invalid", Codes(Kafka(servers)));

    [Fact]
    public void ThePubSubEmulatorAcceptsAComposeServiceName() =>
        Assert.DoesNotContain("profile.gcp.emulator.invalid", Codes(PubSub("pubsub_emulator:8085")));

    [Theory]
    [InlineData("rabbit_mq", true)]
    [InlineData("my_stack_rabbitmq_1", true)]
    [InlineData("rabbit.internal", true)]
    [InlineData("amqp://rabbit_mq", false)]
    [InlineData("rabbit_mq:5672", false)]
    [InlineData("rabbit mq", false)]
    public void RabbitMqAcceptsComposeServiceNamesButNotUrisOrPorts(string host, bool valid) =>
        Assert.Equal(!valid, Codes(Rabbit(host)).Contains("profile.rabbitmq.host.invalid"));
}
