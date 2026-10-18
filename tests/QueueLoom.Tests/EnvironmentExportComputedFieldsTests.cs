using System.Text.Json.Nodes;
using QueueLoom.Core.Profiles;

namespace QueueLoom.Tests;

// The environments export wrote the profile's computed properties too: canWrite (true for a read-write environment
// although the access mode itself is deliberately left out), display names and derived values such as the RabbitMQ
// management address. They are not settings, can contradict what the importer decides, and are now left out.
public sealed class EnvironmentExportComputedFieldsTests
{
    [Fact]
    public void TheExportHoldsSettingsNotComputedValues()
    {
        var rabbit = ServiceBusProfile.CreateNew("Rabbit", EnvironmentKind.Development, new AuthenticationSettings(AuthenticationKind.RabbitMqPassword),
            accessMode: ProfileAccessMode.ReadWrite) with
        {
            Provider = MessagingProvider.RabbitMq, FullyQualifiedNamespace = null, RabbitMq = new RabbitMqSettings("rabbit.internal", "guest", "/", 5672, 15672)
        };
        var kafka = ServiceBusProfile.CreateNew("Kafka", EnvironmentKind.Development, new AuthenticationSettings(AuthenticationKind.KafkaNone)) with
        {
            Provider = MessagingProvider.Kafka, FullyQualifiedNamespace = null, Kafka = new KafkaSettings("broker:9092")
        };

        var environments = JsonNode.Parse(EnvironmentTransfer.Export([rabbit, kafka]))!["environments"]!.AsArray();

        foreach (var environment in environments.Select(node => node!.AsObject()))
        {
            foreach (var computed in new[] { "canWrite", "endpointDisplay", "environmentDisplayName", "authenticationDisplayName" })
                Assert.False(environment.ContainsKey(computed), $"The export wrote the computed '{computed}'.");
        }
        Assert.False(environments[0]!["rabbitMq"]!.AsObject().ContainsKey("managementUri"));
        Assert.False(environments[1]!["kafka"]!.AsObject().ContainsKey("effectiveDeadLetterSuffixes"));
        // What is exported still imports, read-only as always.
        var imported = EnvironmentTransfer.Import(EnvironmentTransfer.Export([rabbit, kafka]), []).Profiles;
        Assert.Equal(2, imported.Count);
        Assert.All(imported, profile => Assert.Equal(ProfileAccessMode.ReadOnly, profile.AccessMode));
    }
}
