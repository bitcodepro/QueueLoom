using System.Globalization;
using QueueLoom.Core.Profiles;

namespace QueueLoom.App.ViewModels;

/// <summary>The self-hosted brokers in the environment editor: RabbitMQ and Apache Kafka.</summary>
public sealed partial class ProfileEditorViewModel
{
    private string _rabbitHost = string.Empty;
    private string _rabbitUserName = string.Empty;
    private string _rabbitVirtualHost = "/";
    private int _rabbitAmqpPort = 5672;
    private int _rabbitManagementPort = 15672;
    private bool _rabbitUseTls;
    private string _brokerPassword = string.Empty;
    private string _kafkaBootstrapServers = string.Empty;
    private bool _kafkaUseTls;
    private bool _kafkaJavaCompatiblePartitioner;
    private bool _kafkaNonIdempotentProducer;
    private KafkaSaslMechanism _kafkaSaslMechanism = KafkaSaslMechanism.ScramSha512;
    private string _kafkaUserName = string.Empty;
    private string _kafkaDeadLetterSuffixes = string.Join(", ", KafkaSettings.DefaultDeadLetterSuffixes);
    private string _schemaRegistryUrl = string.Empty;
    private string _schemaRegistryUserName = string.Empty;
    private string _schemaRegistryPassword = string.Empty;
    private bool _hasExistingSchemaRegistryPassword;
    private string _tlsCaCertificatePath = string.Empty;
    private string _tlsClientCertificatePath = string.Empty;
    private string _tlsClientKeyPath = string.Empty;
    private string _tlsClientCertificatePassword = string.Empty;
    private bool _hasExistingClientCertificate;

    public bool IsRabbitMq => Provider == MessagingProvider.RabbitMq;

    public bool IsKafka => Provider == MessagingProvider.Kafka;

    public string RabbitHost
    {
        get => _rabbitHost;
        set => SetProperty(ref _rabbitHost, value);
    }

    public string RabbitUserName
    {
        get => _rabbitUserName;
        set => SetProperty(ref _rabbitUserName, value);
    }

    public string RabbitVirtualHost
    {
        get => _rabbitVirtualHost;
        set => SetProperty(ref _rabbitVirtualHost, value);
    }

    public int RabbitAmqpPort
    {
        get => _rabbitAmqpPort;
        set => SetProperty(ref _rabbitAmqpPort, value);
    }

    public int RabbitManagementPort
    {
        get => _rabbitManagementPort;
        set => SetProperty(ref _rabbitManagementPort, value);
    }

    /// <summary>Turning TLS on or off moves the ports between their standard plain and TLS values.</summary>
    public bool RabbitUseTls
    {
        get => _rabbitUseTls;
        set
        {
            if (!SetProperty(ref _rabbitUseTls, value))
            {
                return;
            }
            if (RabbitAmqpPort == (value ? 5672 : 5671))
            {
                RabbitAmqpPort = value ? 5671 : 5672;
            }
            if (RabbitManagementPort == (value ? 15672 : 15671))
            {
                RabbitManagementPort = value ? 15671 : 15672;
            }
        }
    }

    /// <summary>The RabbitMQ or Kafka SASL password; it goes to the vault only.</summary>
    public string BrokerPassword
    {
        get => _brokerPassword;
        set => SetProperty(ref _brokerPassword, value);
    }

    public string BrokerPasswordHint => HasExistingSecret
        ? "Leave empty to keep the currently encrypted password."
        : "Stored only in the operating-system-backed QueueLoom vault.";

    public string KafkaBootstrapServers
    {
        get => _kafkaBootstrapServers;
        set => SetProperty(ref _kafkaBootstrapServers, value);
    }

    public bool KafkaUseTls
    {
        get => _kafkaUseTls;
        set => SetProperty(ref _kafkaUseTls, value);
    }

    public bool IsKafkaSasl => AuthenticationKind == AuthenticationKind.KafkaSaslPassword;

    public IReadOnlyList<KafkaSaslMechanism> KafkaSaslMechanisms { get; } = Enum.GetValues<KafkaSaslMechanism>();

    public KafkaSaslMechanism KafkaSaslMechanism
    {
        get => _kafkaSaslMechanism;
        set => SetProperty(ref _kafkaSaslMechanism, value);
    }

    public string KafkaUserName
    {
        get => _kafkaUserName;
        set => SetProperty(ref _kafkaUserName, value);
    }

    /// <summary>Comma-separated topic name endings that mark dead-letter topics.</summary>
    public string KafkaDeadLetterSuffixes
    {
        get => _kafkaDeadLetterSuffixes;
        set => SetProperty(ref _kafkaDeadLetterSuffixes, value);
    }

    /// <summary>Resent keyed records use murmur2 (Java and Spring producers) instead of librdkafka's CRC32.</summary>
    public bool KafkaJavaCompatiblePartitioner
    {
        get => _kafkaJavaCompatiblePartitioner;
        set => SetProperty(ref _kafkaJavaCompatiblePartitioner, value);
    }

    /// <summary>Compatibility mode for clusters that refuse idempotent sends; a lost acknowledgement can then duplicate.</summary>
    public bool KafkaNonIdempotentProducer
    {
        get => _kafkaNonIdempotentProducer;
        set => SetProperty(ref _kafkaNonIdempotentProducer, value);
    }

    /// <summary>Optional Confluent-compatible Schema Registry, used to decode Avro, Protobuf and JSON Schema bodies.</summary>
    public string SchemaRegistryUrl
    {
        get => _schemaRegistryUrl;
        set => SetProperty(ref _schemaRegistryUrl, value);
    }

    public string SchemaRegistryUserName
    {
        get => _schemaRegistryUserName;
        set => SetProperty(ref _schemaRegistryUserName, value);
    }

    /// <summary>The registry password or API secret; it goes to the vault only.</summary>
    public string SchemaRegistryPassword
    {
        get => _schemaRegistryPassword;
        set => SetProperty(ref _schemaRegistryPassword, value);
    }

    public string SchemaRegistryHint => _hasExistingSchemaRegistryPassword
        ? "Leave the password empty to keep the encrypted one. Clear the user name to stop signing in to the registry."
        : "Optional. With a registry, bodies written by Confluent serializers are decoded with their schema. The password or API secret is kept in the vault.";

    /// <summary>A PEM file with the private certificate authority the broker's certificate chains to; empty for the system's.</summary>
    public string TlsCaCertificatePath
    {
        get => _tlsCaCertificatePath;
        set => SetProperty(ref _tlsCaCertificatePath, value);
    }

    /// <summary>The client certificate for mutual TLS: .pfx or .p12, or PEM.</summary>
    public string TlsClientCertificatePath
    {
        get => _tlsClientCertificatePath;
        set => SetProperty(ref _tlsClientCertificatePath, value);
    }

    /// <summary>The key of a PEM client certificate, when it is in a separate file.</summary>
    public string TlsClientKeyPath
    {
        get => _tlsClientKeyPath;
        set => SetProperty(ref _tlsClientKeyPath, value);
    }

    /// <summary>The client certificate's password; it goes to the vault only.</summary>
    public string TlsClientCertificatePassword
    {
        get => _tlsClientCertificatePassword;
        set => SetProperty(ref _tlsClientCertificatePassword, value);
    }

    public string TlsHint => _hasExistingClientCertificate
        ? "Leave the password empty to keep the encrypted one. Clear the client certificate to stop using it and its password."
        : "Optional. A certificate authority for a broker certificate the system does not trust, and a client certificate when the broker asks for one (mutual TLS). Only the file paths are saved; the password is kept in the vault.";

    private void InitializeBrokers(ServiceBusProfile? existing)
    {
        if (existing?.RabbitMq is { } rabbit)
        {
            _rabbitHost = rabbit.Host;
            _rabbitUserName = rabbit.UserName;
            _rabbitVirtualHost = rabbit.VirtualHost;
            _rabbitAmqpPort = rabbit.AmqpPort;
            _rabbitManagementPort = rabbit.ManagementPort;
            _rabbitUseTls = rabbit.UseTls;
            InitializeTls(rabbit.Tls);
        }
        if (existing?.Kafka is { } kafka)
        {
            _kafkaBootstrapServers = kafka.BootstrapServers;
            _kafkaUseTls = kafka.UseTls;
            _kafkaSaslMechanism = kafka.SaslMechanism ?? KafkaSaslMechanism.ScramSha512;
            _kafkaUserName = kafka.UserName ?? string.Empty;
            _kafkaDeadLetterSuffixes = string.Join(", ", kafka.EffectiveDeadLetterSuffixes);
            _schemaRegistryUrl = kafka.SchemaRegistryUrl ?? string.Empty;
            _kafkaJavaCompatiblePartitioner = kafka.JavaCompatiblePartitioner;
            _kafkaNonIdempotentProducer = kafka.NonIdempotentProducer;
            _schemaRegistryUserName = kafka.SchemaRegistryUserName ?? string.Empty;
            _hasExistingSchemaRegistryPassword = kafka.SchemaRegistryUserName is not null;
            InitializeTls(kafka.Tls);
        }
    }

    private (ServiceBusProfile Profile, string? Secret)? BuildRabbitMq()
    {
        if (!TakePassword(out var newSecret))
        {
            return null;
        }

        var settings = new RabbitMqSettings(
            RabbitHost.Trim(),
            RabbitUserName.Trim(),
            string.IsNullOrWhiteSpace(RabbitVirtualHost) ? "/" : RabbitVirtualHost.Trim(),
            RabbitAmqpPort,
            RabbitManagementPort,
            RabbitUseTls,
            BuildTls(RabbitUseTls));
        return (NewProfile(new AuthenticationSettings(AuthenticationKind), null) with { RabbitMq = settings }, newSecret);
    }

    private (ServiceBusProfile Profile, string? Secret)? BuildKafka()
    {
        string? newSecret = null;
        if (IsKafkaSasl && !TakePassword(out newSecret))
        {
            return null;
        }

        var suffixes = KafkaDeadLetterSuffixes
            .Split([',', ' ', ';'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        var settings = new KafkaSettings(
            string.Join(",", KafkaBootstrapServers.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)),
            KafkaUseTls,
            IsKafkaSasl ? KafkaSaslMechanism : null,
            IsKafkaSasl ? NullIfWhiteSpace(KafkaUserName) : null,
            suffixes.SequenceEqual(KafkaSettings.DefaultDeadLetterSuffixes) ? null : suffixes,
            NullIfWhiteSpace(SchemaRegistryUrl)?.TrimEnd('/'),
            NullIfWhiteSpace(SchemaRegistryUrl) is null ? null : NullIfWhiteSpace(SchemaRegistryUserName),
            KafkaJavaCompatiblePartitioner,
            KafkaNonIdempotentProducer,
            BuildTls(KafkaUseTls));
        if (settings.SchemaRegistryUserName is not null && string.IsNullOrEmpty(SchemaRegistryPassword) && !_hasExistingSchemaRegistryPassword)
        {
            Error = "Enter the Schema Registry password or API secret.";
            return null;
        }
        return (NewProfile(new AuthenticationSettings(AuthenticationKind), null) with { Kafka = settings }, newSecret);
    }

    /// <summary>What happens to the registry password in the vault: a new value, removal, or nothing.</summary>
    private (string? Password, bool Remove) SchemaRegistrySecret(ServiceBusProfile profile) =>
        profile.Kafka?.SchemaRegistryUserName is null
            ? (null, _hasExistingSchemaRegistryPassword)
            : (string.IsNullOrEmpty(SchemaRegistryPassword) ? null : SchemaRegistryPassword, false);

    private void InitializeTls(TlsClientSettings? tls)
    {
        _tlsCaCertificatePath = tls?.CaCertificatePath ?? string.Empty;
        _tlsClientCertificatePath = tls?.ClientCertificatePath ?? string.Empty;
        _tlsClientKeyPath = tls?.ClientKeyPath ?? string.Empty;
        _hasExistingClientCertificate = tls?.ClientCertificatePath is not null;
    }

    /// <summary>The certificate files, kept only with TLS on; none at all is saved as no settings.</summary>
    private TlsClientSettings? BuildTls(bool useTls)
    {
        if (!useTls)
        {
            return null;
        }
        var certificate = NullIfWhiteSpace(TlsClientCertificatePath);
        var tls = new TlsClientSettings(NullIfWhiteSpace(TlsCaCertificatePath), certificate, certificate is null ? null : NullIfWhiteSpace(TlsClientKeyPath));
        return tls.IsEmpty ? null : tls;
    }

    /// <summary>What happens to the client certificate password in the vault: a new value, removal, or nothing.</summary>
    private (string? Password, bool Remove) ClientCertificateSecret(ServiceBusProfile profile) =>
        (profile.Kafka?.Tls ?? profile.RabbitMq?.Tls)?.ClientCertificatePath is null
            ? (null, _hasExistingClientCertificate)
            : (string.IsNullOrEmpty(TlsClientCertificatePassword) ? null : TlsClientCertificatePassword, false);

    private bool TakePassword(out string? newSecret)
    {
        newSecret = null;
        if (!string.IsNullOrEmpty(BrokerPassword))
        {
            newSecret = BrokerPassword;
            return true;
        }
        if (HasExistingSecret)
        {
            return true;
        }
        Error = "Enter the password.";
        return false;
    }

    internal static string Port(int port) => port.ToString(CultureInfo.InvariantCulture);
}
