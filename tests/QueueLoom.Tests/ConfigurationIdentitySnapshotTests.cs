using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Tests;

// A profile's configuration identity is a hash of its serialized form (ScheduledResend.IdentityFor). Scheduled resends
// and recoverable batch operations store it and refuse to run when it changes, so the serialized form of an unchanged
// profile must not change between versions. A new property on a profile or on its settings changes it silently, as a
// derived getter without [JsonIgnore] nearly did (#117). These snapshots pin the form and the identity of a fully
// filled profile of every provider.
//
// If this test fails after adding a property: mark a derived property [JsonIgnore]; for a real setting, omit it while
// it has its default ([JsonIgnore(Condition = WhenWritingDefault)], as KafkaSettings.JavaCompatiblePartitioner does) so
// existing profiles keep their identity. Only update a snapshot when the change is meant to invalidate saved work.
public sealed class ConfigurationIdentitySnapshotTests
{
    private static readonly Guid Id = Guid.Parse("6f9b2a8e-1c3d-4e5f-8a7b-9c0d1e2f3a4b");
    private static readonly Guid Revision = Guid.Parse("0a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d");

    // Produced by the version this test was added in (main ee38475 plus #117, RabbitMQ HostName ignored).
    public static TheoryData<string, string, string> Snapshots => new()
    {
        {
            "azure",
            """{"Id":"6f9b2a8e-1c3d-4e5f-8a7b-9c0d1e2f3a4b","Name":"Orders","Environment":2,"CustomEnvironmentName":"Prod EU","FullyQualifiedNamespace":"orders.servicebus.windows.net","Authentication":{"Kind":0,"EntraId":null},"AccessMode":1,"EmulatorManagementPort":5301,"ConfigurationRevision":"0a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d","Provider":0,"Aws":null,"GooglePubSub":null,"RabbitMq":null,"Kafka":null,"EndpointDisplay":"orders.servicebus.windows.net","EnvironmentDisplayName":"Production","AuthenticationDisplayName":"SAS connection string","CanWrite":true,"AllowQueueManagement":true}""",
            "4645AF9230FC47D72B047B81CD6940E1A0A08FBAFDE804F39354DEDDA5E115F0"
        },
        {
            "azure-entra",
            """{"Id":"6f9b2a8e-1c3d-4e5f-8a7b-9c0d1e2f3a4b","Name":"Orders","Environment":2,"CustomEnvironmentName":"Prod EU","FullyQualifiedNamespace":"orders.servicebus.windows.net","Authentication":{"Kind":1,"EntraId":{"CredentialKind":0,"TenantId":"tenant-1","ClientId":"client-1"}},"AccessMode":1,"EmulatorManagementPort":5301,"ConfigurationRevision":"0a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d","Provider":0,"Aws":null,"GooglePubSub":null,"RabbitMq":null,"Kafka":null,"EndpointDisplay":"orders.servicebus.windows.net","EnvironmentDisplayName":"Production","AuthenticationDisplayName":"Entra ID \u00B7 DefaultAzureCredential","CanWrite":true,"AllowQueueManagement":true}""",
            "C7CD7D7457FC36652322D0E601491B4ACEC47BEE6179B4AFCC54ECA13BE0E12E"
        },
        {
            "aws",
            """{"Id":"6f9b2a8e-1c3d-4e5f-8a7b-9c0d1e2f3a4b","Name":"Orders","Environment":2,"CustomEnvironmentName":"Prod EU","FullyQualifiedNamespace":null,"Authentication":{"Kind":2,"EntraId":null},"AccessMode":1,"EmulatorManagementPort":5301,"ConfigurationRevision":"0a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d","Provider":1,"Aws":{"Region":"eu-central-1","ServiceUrl":"http://localhost:4566","ProfileName":"orders"},"GooglePubSub":null,"RabbitMq":null,"Kafka":null,"EndpointDisplay":"eu-central-1 \u00B7 http://localhost:4566","EnvironmentDisplayName":"Production","AuthenticationDisplayName":"AWS access key","CanWrite":true,"AllowQueueManagement":true}""",
            "E299C653A9BD985DA4C3383D6EFDFC5ABB7A00B0B4D93698BD0F57DDF60F221E"
        },
        {
            "pubsub",
            """{"Id":"6f9b2a8e-1c3d-4e5f-8a7b-9c0d1e2f3a4b","Name":"Orders","Environment":2,"CustomEnvironmentName":"Prod EU","FullyQualifiedNamespace":null,"Authentication":{"Kind":4,"EntraId":null},"AccessMode":1,"EmulatorManagementPort":5301,"ConfigurationRevision":"0a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d","Provider":2,"Aws":null,"GooglePubSub":{"ProjectId":"orders-prod-4821","EmulatorHost":"localhost:8085"},"RabbitMq":null,"Kafka":null,"EndpointDisplay":"orders-prod-4821 \u00B7 emulator localhost:8085","EnvironmentDisplayName":"Production","AuthenticationDisplayName":"Service account key","CanWrite":true,"AllowQueueManagement":true}""",
            "D50DC24725C01F8CF16791FB638277D9FAC8AE231B404A3428C858D5E5430B7E"
        },
        {
            "rabbitmq",
            """{"Id":"6f9b2a8e-1c3d-4e5f-8a7b-9c0d1e2f3a4b","Name":"Orders","Environment":2,"CustomEnvironmentName":"Prod EU","FullyQualifiedNamespace":null,"Authentication":{"Kind":6,"EntraId":null},"AccessMode":1,"EmulatorManagementPort":5301,"ConfigurationRevision":"0a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d","Provider":3,"Aws":null,"GooglePubSub":null,"RabbitMq":{"Host":"rabbit.internal","UserName":"orders","VirtualHost":"billing","AmqpPort":5671,"ManagementPort":15671,"UseTls":true,"ManagementUri":"https://rabbit.internal:15671/"},"Kafka":null,"EndpointDisplay":"rabbit.internal:5671 \u00B7 vhost billing","EnvironmentDisplayName":"Production","AuthenticationDisplayName":"User orders","CanWrite":true,"AllowQueueManagement":true}""",
            "6D779F75984EC84CE88C92554BCC5E5FFAD33BC669A543D42B3F968F01C9C550"
        },
        {
            "kafka",
            """{"Id":"6f9b2a8e-1c3d-4e5f-8a7b-9c0d1e2f3a4b","Name":"Orders","Environment":2,"CustomEnvironmentName":"Prod EU","FullyQualifiedNamespace":null,"Authentication":{"Kind":8,"EntraId":null},"AccessMode":1,"EmulatorManagementPort":5301,"ConfigurationRevision":"0a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d","Provider":4,"Aws":null,"GooglePubSub":null,"RabbitMq":null,"Kafka":{"BootstrapServers":"broker-1:9093,broker-2:9093","UseTls":true,"SaslMechanism":2,"UserName":"orders","DeadLetterSuffixes":[".DLT"],"SchemaRegistryUrl":"https://registry.internal","SchemaRegistryUserName":"registry-user","EffectiveDeadLetterSuffixes":[".DLT"]},"EndpointDisplay":"broker-1:9093,broker-2:9093","EnvironmentDisplayName":"Production","AuthenticationDisplayName":"SASL ScramSha512 \u00B7 orders","CanWrite":true,"AllowQueueManagement":true}""",
            "51F9AE360246B43DF260D5EB563E5B44477D82119168F22E7D9359DC04586B86"
        }
    };

    private static ServiceBusProfile Profile(string provider)
    {
        var profile = new ServiceBusProfile(Id, "Orders", EnvironmentKind.Production, "Prod EU", "orders.servicebus.windows.net",
            new AuthenticationSettings(AuthenticationKind.ConnectionString), ProfileAccessMode.ReadWrite)
        {
            ConfigurationRevision = Revision,
            EmulatorManagementPort = 5301,
            AllowQueueManagement = true
        };
        return provider switch
        {
            "azure" => profile,
            "azure-entra" => profile with
            {
                Authentication = AuthenticationSettings.Entra(EntraIdCredentialKind.DefaultAzureCredential, "tenant-1", "client-1")
            },
            "aws" => profile with
            {
                Provider = MessagingProvider.AmazonSqsSns, FullyQualifiedNamespace = null,
                Authentication = new AuthenticationSettings(AuthenticationKind.AwsAccessKey),
                Aws = new AwsSettings("eu-central-1", "http://localhost:4566", "orders")
            },
            "pubsub" => profile with
            {
                Provider = MessagingProvider.GooglePubSub, FullyQualifiedNamespace = null,
                Authentication = new AuthenticationSettings(AuthenticationKind.GoogleServiceAccountKey),
                GooglePubSub = new GooglePubSubSettings("orders-prod-4821", "localhost:8085")
            },
            "rabbitmq" => profile with
            {
                Provider = MessagingProvider.RabbitMq, FullyQualifiedNamespace = null,
                Authentication = new AuthenticationSettings(AuthenticationKind.RabbitMqPassword),
                RabbitMq = new RabbitMqSettings("rabbit.internal", "orders", "billing", 5671, 15671, UseTls: true)
            },
            _ => profile with
            {
                Provider = MessagingProvider.Kafka, FullyQualifiedNamespace = null,
                Authentication = new AuthenticationSettings(AuthenticationKind.KafkaSaslPassword),
                Kafka = new KafkaSettings("broker-1:9093,broker-2:9093", UseTls: true, KafkaSaslMechanism.ScramSha512, "orders",
                    [".DLT"], "https://registry.internal", "registry-user")
            }
        };
    }

    [Theory]
    [MemberData(nameof(Snapshots))]
    public void AnUnchangedProfileKeepsItsSerializedFormAndConfigurationIdentity(string provider, string json, string identity)
    {
        var profile = Profile(provider);

        Assert.Equal(json, System.Text.Json.JsonSerializer.Serialize(profile));
        Assert.Equal(identity, ScheduledResend.IdentityFor(profile));
    }
}
