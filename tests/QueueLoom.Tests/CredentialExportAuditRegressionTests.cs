using Confluent.Kafka;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Core.Settings;
using QueueLoom.Infrastructure.Kafka;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class CredentialExportAuditRegressionTests
{
    [Theory]
    [InlineData("https://dummy:dummy@registry.invalid")]
    [InlineData("https://registry.invalid?token=dummy")]
    public void SchemaRegistry_RejectsCredentialBearingUrl(string url)
    {
        Assert.False(ProfileValidator.Validate(KafkaProfile(url)).IsValid);
    }

    [Theory]
    [InlineData("https://dummy:dummy@registry.invalid")]
    [InlineData("https://registry.invalid?token=dummy")]
    public void EnvironmentExport_RejectsLegacyCredentialBearingRegistryUrl(string url)
    {
        Assert.Throws<InvalidOperationException>(() => EnvironmentTransfer.Export([KafkaProfile(url)]));
    }

    private static ServiceBusProfile KafkaProfile(string url) =>
        ServiceBusProfile.CreateNew("Events", EnvironmentKind.Development, new AuthenticationSettings(AuthenticationKind.KafkaNone)) with
        { Provider = MessagingProvider.Kafka, Kafka = new KafkaSettings("broker.invalid:9092") { SchemaRegistryUrl = url } };

}
