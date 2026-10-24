using QueueLoom.Core.Profiles;

namespace QueueLoom.Tests;

public sealed class EnvironmentTransferTests
{
    [Fact]
    public void Export_CarriesSettingsButNoIdsWriteAccessOrSecrets()
    {
        var kafka = ServiceBusProfile.CreateNew("Kafka prod", EnvironmentKind.Production,
                new AuthenticationSettings(AuthenticationKind.KafkaSaslPassword), accessMode: ProfileAccessMode.ReadWrite) with
        {
            Provider = MessagingProvider.Kafka,
            Kafka = new KafkaSettings("broker:9092", true, KafkaSaslMechanism.ScramSha512, "svc", SchemaRegistryUrl: "https://registry")
        };

        var json = EnvironmentTransfer.Export([kafka]);

        Assert.DoesNotContain(kafka.Id.ToString(), json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("accessMode", json, StringComparison.Ordinal);
        Assert.Contains("\"format\": \"queueloom-environments\"", json, StringComparison.Ordinal);
        Assert.Contains("https://registry", json, StringComparison.Ordinal);
    }

    [Fact]
    public void Export_RefusalNamesTheEnvironmentWithCredentialsInItsRegistryUrl()
    {
        ServiceBusProfile Kafka(string name, string registry) =>
            ServiceBusProfile.CreateNew(name, EnvironmentKind.Test, new AuthenticationSettings(AuthenticationKind.KafkaSaslPassword)) with
            {
                Provider = MessagingProvider.Kafka,
                Kafka = new KafkaSettings("broker:9092", true, KafkaSaslMechanism.ScramSha512, "svc", SchemaRegistryUrl: registry)
            };

        var exception = Assert.Throws<InvalidOperationException>(() =>
            EnvironmentTransfer.Export([Kafka("Dev Kafka", "https://registry"), Kafka("Staging Kafka", "https://u:p@reg")]));

        Assert.Contains("Staging Kafka", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("Dev Kafka", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("u:p", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("reg/", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("https://", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Export_RefusalListsAtMostFiveEnvironments()
    {
        var profiles = Enumerable.Range(1, 7).Select(i =>
            ServiceBusProfile.CreateNew($"Kafka {i}", EnvironmentKind.Test, new AuthenticationSettings(AuthenticationKind.KafkaSaslPassword)) with
            {
                Provider = MessagingProvider.Kafka,
                Kafka = new KafkaSettings("broker:9092", true, KafkaSaslMechanism.ScramSha512, "svc", SchemaRegistryUrl: "https://reg?key=x")
            }).ToList();

        var exception = Assert.Throws<InvalidOperationException>(() => EnvironmentTransfer.Export(profiles));

        Assert.Contains("Kafka 5", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("Kafka 6", exception.Message, StringComparison.Ordinal);
        Assert.Contains("and 2 more", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Import_GivesNewIdsReadOnlyAccessAndUniqueNames()
    {
        var rabbit = ServiceBusProfile.CreateNew("Rabbit", EnvironmentKind.Test,
                new AuthenticationSettings(AuthenticationKind.RabbitMqPassword), accessMode: ProfileAccessMode.ReadWrite) with
        {
            Provider = MessagingProvider.RabbitMq,
            RabbitMq = new RabbitMqSettings("rabbit.local", "ops")
        };
        var json = EnvironmentTransfer.Export([rabbit]);

        var import = EnvironmentTransfer.Import(json, [rabbit]);

        var profile = Assert.Single(import.Profiles);
        Assert.NotEqual(rabbit.Id, profile.Id);
        Assert.Equal("Rabbit (2)", profile.Name);
        Assert.Equal(ProfileAccessMode.ReadOnly, profile.AccessMode);
        Assert.Equal(rabbit.RabbitMq, profile.RabbitMq);
        Assert.Equal(1, import.NeedSecrets);
        Assert.Empty(import.Skipped);
    }

    [Fact]
    public void Import_SkipsInvalidEnvironmentsAndRejectsOtherFiles()
    {
        var json = """
            {"format":"queueloom-environments","version":1,"environments":[
              {"name":"Broken","environment":"Test","authentication":{"kind":"KafkaNone"},"provider":"Kafka","kafka":{"bootstrapServers":""}}
            ]}
            """;

        var import = EnvironmentTransfer.Import(json, []);

        Assert.Empty(import.Profiles);
        Assert.StartsWith("Broken:", Assert.Single(import.Skipped), StringComparison.Ordinal);
        Assert.Throws<InvalidOperationException>(() => EnvironmentTransfer.Import("""{"profiles":[]}""", []));
        Assert.Throws<InvalidOperationException>(() => EnvironmentTransfer.Import("not json", []));
    }
}
