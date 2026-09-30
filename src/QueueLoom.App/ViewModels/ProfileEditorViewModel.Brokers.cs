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
    private KafkaSaslMechanism _kafkaSaslMechanism = KafkaSaslMechanism.ScramSha512;
    private string _kafkaUserName = string.Empty;
    private string _kafkaDeadLetterSuffixes = string.Join(", ", KafkaSettings.DefaultDeadLetterSuffixes);

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
        }
        if (existing?.Kafka is { } kafka)
        {
            _kafkaBootstrapServers = kafka.BootstrapServers;
            _kafkaUseTls = kafka.UseTls;
            _kafkaSaslMechanism = kafka.SaslMechanism ?? KafkaSaslMechanism.ScramSha512;
            _kafkaUserName = kafka.UserName ?? string.Empty;
            _kafkaDeadLetterSuffixes = string.Join(", ", kafka.EffectiveDeadLetterSuffixes);
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
            RabbitUseTls);
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
            suffixes.SequenceEqual(KafkaSettings.DefaultDeadLetterSuffixes) ? null : suffixes);
        return (NewProfile(new AuthenticationSettings(AuthenticationKind), null) with { Kafka = settings }, newSecret);
    }

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
