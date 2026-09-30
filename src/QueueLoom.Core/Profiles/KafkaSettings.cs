namespace QueueLoom.Core.Profiles;

public enum KafkaSaslMechanism
{
    Plain,
    ScramSha256,
    ScramSha512
}

/// <summary>Non-secret settings of a Kafka cluster; a SASL password is kept in the secret vault.</summary>
/// <param name="BootstrapServers">Comma-separated host:port list, for example broker-1:9092,broker-2:9092.</param>
/// <param name="UseTls">Connect with TLS (SSL or SASL_SSL).</param>
/// <param name="SaslMechanism">Set together with <paramref name="UserName"/> when the cluster requires SASL.</param>
/// <param name="DeadLetterSuffixes">
/// Topic name endings that mark dead-letter topics, for example ".DLT" (Spring) and "-dlq" (Kafka Connect).
/// "orders.DLT" is then shown as the dead-letter queue of "orders".
/// </param>
/// <param name="SchemaRegistryUrl">
/// A Confluent-compatible Schema Registry. Bodies written by its serializers (a zero byte and a schema id first) are
/// then decoded with their Avro, Protobuf or JSON schema.
/// </param>
/// <param name="SchemaRegistryUserName">The API key or user for the registry; its secret is kept in the vault.</param>
public sealed record KafkaSettings(
    string BootstrapServers,
    bool UseTls = false,
    KafkaSaslMechanism? SaslMechanism = null,
    string? UserName = null,
    IReadOnlyList<string>? DeadLetterSuffixes = null,
    string? SchemaRegistryUrl = null,
    string? SchemaRegistryUserName = null)
{
    public static readonly IReadOnlyList<string> DefaultDeadLetterSuffixes = [".DLT", "-dlt", ".dlq", "-dlq", "_dlq", ".DLQ"];

    public IReadOnlyList<string> EffectiveDeadLetterSuffixes =>
        DeadLetterSuffixes is { Count: > 0 } ? DeadLetterSuffixes : DefaultDeadLetterSuffixes;
}
