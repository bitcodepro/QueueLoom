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
