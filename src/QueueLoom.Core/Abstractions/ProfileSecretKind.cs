namespace QueueLoom.Core.Abstractions;

public enum ProfileSecretKind
{
    ConnectionString,

    /// <summary>The password or API secret of a Kafka Schema Registry.</summary>
    SchemaRegistryPassword,

    /// <summary>The password of a client certificate's private key (mutual TLS).</summary>
    ClientCertificatePassword
}
