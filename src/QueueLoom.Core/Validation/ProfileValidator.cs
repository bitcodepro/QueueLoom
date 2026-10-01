using System.Text.RegularExpressions;
using QueueLoom.Core.Profiles;

namespace QueueLoom.Core.Validation;

public static partial class ProfileValidator
{
    public const int MaxNameLength = 100;
    public const int MaxEnvironmentNameLength = 50;

    public static ValidationResult Validate(ServiceBusProfile? profile) =>
        Validate(profile, allowLegacyRegistryUrl: false);

    /// <summary>
    /// Loads previously accepted registry URLs without hiding environments or altering credentials.
    /// New/edited profiles and environment transfer continue to use strict validation.
    /// </summary>
    public static ValidationResult ValidatePersistedProfile(ServiceBusProfile? profile) =>
        Validate(profile, allowLegacyRegistryUrl: true);

    private static ValidationResult Validate(ServiceBusProfile? profile, bool allowLegacyRegistryUrl)
    {
        if (profile is null)
        {
            return new ValidationResult(
                [new ValidationError("profile.required", "A profile is required.")]);
        }

        var errors = new List<ValidationError>();

        if (profile.Id == Guid.Empty)
        {
            errors.Add(new ValidationError(
                "profile.id.required",
                "The profile identifier must not be empty.",
                nameof(profile.Id)));
        }

        ValidateName(profile.Name, errors);
        ValidateEnvironment(profile, errors);
        ValidateAccessMode(profile, errors);
        ValidateProvider(profile, errors);
        ValidateAuthentication(profile, errors);
        switch (profile.Provider)
        {
            case MessagingProvider.AmazonSqsSns:
                ValidateAws(profile, errors);
                break;
            case MessagingProvider.GooglePubSub:
                ValidateGooglePubSub(profile, errors);
                break;
            case MessagingProvider.RabbitMq:
                ValidateRabbitMq(profile, errors);
                break;
            case MessagingProvider.Kafka:
                ValidateKafka(profile, errors, allowLegacyRegistryUrl);
                break;
            default:
                ValidateNamespace(profile, errors);
                break;
        }

        return errors.Count == 0 ? ValidationResult.Valid : new ValidationResult(errors);
    }

    public static string NormalizeFullyQualifiedNamespace(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);

        var normalized = value.Trim();
        if (normalized.StartsWith("sb://", StringComparison.OrdinalIgnoreCase))
        {
            normalized = normalized[5..];
        }

        return normalized.TrimEnd('/').ToLowerInvariant();
    }

    private static void ValidateName(string? name, ICollection<ValidationError> errors)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            errors.Add(new ValidationError(
                "profile.name.required",
                "A profile name is required.",
                nameof(ServiceBusProfile.Name)));
        }
        else if (name.Trim().Length > MaxNameLength)
        {
            errors.Add(new ValidationError(
                "profile.name.too_long",
                $"The profile name cannot exceed {MaxNameLength} characters.",
                nameof(ServiceBusProfile.Name)));
        }
    }

    private static void ValidateEnvironment(
        ServiceBusProfile profile,
        ICollection<ValidationError> errors)
    {
        if (!Enum.IsDefined(profile.Environment))
        {
            errors.Add(new ValidationError(
                "profile.environment.invalid",
                "The environment is not supported.",
                nameof(profile.Environment)));
            return;
        }

        if (profile.Environment != EnvironmentKind.Custom)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(profile.CustomEnvironmentName))
        {
            errors.Add(new ValidationError(
                "profile.environment_name.required",
                "A custom environment name is required.",
                nameof(profile.CustomEnvironmentName)));
        }
        else if (profile.CustomEnvironmentName.Trim().Length > MaxEnvironmentNameLength)
        {
            errors.Add(new ValidationError(
                "profile.environment_name.too_long",
                $"The custom environment name cannot exceed {MaxEnvironmentNameLength} characters.",
                nameof(profile.CustomEnvironmentName)));
        }
    }

    private static void ValidateAccessMode(
        ServiceBusProfile profile,
        ICollection<ValidationError> errors)
    {
        if (!Enum.IsDefined(profile.AccessMode))
        {
            errors.Add(new ValidationError(
                "profile.access_mode.invalid",
                "The profile access mode is not supported.",
                nameof(profile.AccessMode)));
            return;
        }

        if (profile.Environment == EnvironmentKind.Production &&
            profile.AccessMode != ProfileAccessMode.ReadOnly)
        {
            errors.Add(new ValidationError(
                "profile.production.read_only",
                "Production profiles must be persisted as read-only.",
                nameof(profile.AccessMode)));
        }
    }

    private static void ValidateProvider(
        ServiceBusProfile profile,
        ICollection<ValidationError> errors)
    {
        if (!Enum.IsDefined(profile.Provider))
        {
            errors.Add(new ValidationError(
                "profile.provider.invalid",
                "The messaging service is not supported.",
                nameof(profile.Provider)));
            return;
        }

        if (profile.Provider != MessagingProvider.AmazonSqsSns && profile.Aws is not null ||
            profile.Provider != MessagingProvider.GooglePubSub && profile.GooglePubSub is not null ||
            profile.Provider != MessagingProvider.RabbitMq && profile.RabbitMq is not null ||
            profile.Provider != MessagingProvider.Kafka && profile.Kafka is not null)
        {
            errors.Add(new ValidationError(
                "profile.provider.settings_unexpected",
                "The environment contains settings for a different messaging service.",
                nameof(profile.Provider)));
        }

        if (profile.Provider != MessagingProvider.AzureServiceBus &&
            !string.IsNullOrWhiteSpace(profile.FullyQualifiedNamespace))
        {
            errors.Add(new ValidationError(
                "profile.namespace.unexpected",
                "A Service Bus namespace only applies to Azure Service Bus environments.",
                nameof(profile.FullyQualifiedNamespace)));
        }

        if (profile.Authentication is not null &&
            Enum.IsDefined(profile.Authentication.Kind) &&
            profile.Authentication.Kind.Provider() != profile.Provider)
        {
            errors.Add(new ValidationError(
                "profile.authentication.provider_mismatch",
                $"This sign-in method cannot be used with {profile.Provider.DisplayName()}.",
                nameof(profile.Authentication)));
        }
    }

    private static void ValidateAws(
        ServiceBusProfile profile,
        ICollection<ValidationError> errors)
    {
        var settings = profile.Aws;
        if (settings is null || string.IsNullOrWhiteSpace(settings.Region))
        {
            errors.Add(new ValidationError(
                "profile.aws.region.required",
                "An AWS region is required, for example eu-west-1.",
                nameof(profile.Aws)));
            return;
        }

        if (!AwsRegionPattern().IsMatch(settings.Region))
        {
            errors.Add(new ValidationError(
                "profile.aws.region.invalid",
                "Use an AWS region code such as us-east-1 or eu-central-1.",
                nameof(profile.Aws)));
        }

        if (!string.IsNullOrWhiteSpace(settings.ServiceUrl) &&
            (!Uri.TryCreate(settings.ServiceUrl, UriKind.Absolute, out var uri) ||
             uri.Scheme is not ("http" or "https") ||
             !string.IsNullOrEmpty(uri.Query) ||
             !string.IsNullOrEmpty(uri.UserInfo)))
        {
            errors.Add(new ValidationError(
                "profile.aws.service_url.invalid",
                "The endpoint must be an http or https address, for example http://localhost:4566.",
                nameof(profile.Aws)));
        }

        if (!string.IsNullOrWhiteSpace(settings.ProfileName) &&
            (settings.ProfileName.Trim().Length > 64 || settings.ProfileName.Any(char.IsWhiteSpace)))
        {
            errors.Add(new ValidationError(
                "profile.aws.profile_name.invalid",
                "The AWS profile name cannot contain spaces or exceed 64 characters.",
                nameof(profile.Aws)));
        }
    }

    private static void ValidateGooglePubSub(
        ServiceBusProfile profile,
        ICollection<ValidationError> errors)
    {
        var settings = profile.GooglePubSub;
        if (settings is null || string.IsNullOrWhiteSpace(settings.ProjectId))
        {
            errors.Add(new ValidationError(
                "profile.gcp.project.required",
                "A Google Cloud project ID is required.",
                nameof(profile.GooglePubSub)));
            return;
        }

        if (!GoogleProjectPattern().IsMatch(settings.ProjectId))
        {
            errors.Add(new ValidationError(
                "profile.gcp.project.invalid",
                "Use the project ID (not the display name), for example orders-prod-4821.",
                nameof(profile.GooglePubSub)));
        }

        if (!string.IsNullOrWhiteSpace(settings.EmulatorHost) &&
            !HostAndPortPattern().IsMatch(settings.EmulatorHost))
        {
            errors.Add(new ValidationError(
                "profile.gcp.emulator.invalid",
                "The emulator address must be host:port, for example localhost:8085.",
                nameof(profile.GooglePubSub)));
        }
    }

    private static void ValidateRabbitMq(ServiceBusProfile profile, ICollection<ValidationError> errors)
    {
        var settings = profile.RabbitMq;
        if (settings is null || string.IsNullOrWhiteSpace(settings.Host))
        {
            errors.Add(new ValidationError("profile.rabbitmq.host.required",
                "The RabbitMQ host is required, for example rabbit.internal or localhost.", nameof(profile.RabbitMq)));
            return;
        }
        if (Uri.CheckHostName(settings.Host.Trim()) == UriHostNameType.Unknown)
        {
            errors.Add(new ValidationError("profile.rabbitmq.host.invalid",
                "Enter only the host name, without amqp:// or a port.", nameof(profile.RabbitMq)));
        }
        if (string.IsNullOrWhiteSpace(settings.UserName))
        {
            errors.Add(new ValidationError("profile.rabbitmq.user.required", "The RabbitMQ user name is required.", nameof(profile.RabbitMq)));
        }
        if (string.IsNullOrWhiteSpace(settings.VirtualHost))
        {
            errors.Add(new ValidationError("profile.rabbitmq.vhost.required",
                "The virtual host is required; the default one is \"/\".", nameof(profile.RabbitMq)));
        }
        if (settings.AmqpPort is < 1 or > 65_535 || settings.ManagementPort is < 1 or > 65_535)
        {
            errors.Add(new ValidationError("profile.rabbitmq.port.invalid", "Ports must be between 1 and 65535.", nameof(profile.RabbitMq)));
        }
    }

    private static void ValidateKafka(ServiceBusProfile profile, ICollection<ValidationError> errors, bool allowLegacyRegistryUrl)
    {
        var settings = profile.Kafka;
        if (settings is null || string.IsNullOrWhiteSpace(settings.BootstrapServers))
        {
            errors.Add(new ValidationError("profile.kafka.servers.required",
                "At least one bootstrap server is required, for example broker-1:9092.", nameof(profile.Kafka)));
            return;
        }
        if (settings.BootstrapServers.Split(',', StringSplitOptions.TrimEntries).Any(server => !HostAndPortPattern().IsMatch(server)))
        {
            errors.Add(new ValidationError("profile.kafka.servers.invalid",
                "List the servers as host:port separated by commas, for example broker-1:9092,broker-2:9092.", nameof(profile.Kafka)));
        }
        var usesSasl = profile.Authentication?.Kind == AuthenticationKind.KafkaSaslPassword;
        if (usesSasl && (settings.SaslMechanism is null || string.IsNullOrWhiteSpace(settings.UserName)))
        {
            errors.Add(new ValidationError("profile.kafka.sasl.incomplete",
                "SASL sign-in needs a mechanism and a user name.", nameof(profile.Kafka)));
        }
        if (settings.DeadLetterSuffixes?.Any(suffix => string.IsNullOrWhiteSpace(suffix) || suffix.Any(char.IsWhiteSpace)) == true)
        {
            errors.Add(new ValidationError("profile.kafka.dlq_suffix.invalid",
                "Dead-letter topic endings cannot be empty or contain spaces.", nameof(profile.Kafka)));
        }
        if (settings.SchemaRegistryUrl is { } registry &&
            (!Uri.TryCreate(registry, UriKind.Absolute, out var registryUri) || registryUri.Scheme is not ("http" or "https") ||
             !allowLegacyRegistryUrl && (!string.IsNullOrEmpty(registryUri.UserInfo) || !string.IsNullOrEmpty(registryUri.Query) || !string.IsNullOrEmpty(registryUri.Fragment))))
        {
            errors.Add(new ValidationError("profile.kafka.schema_registry.invalid",
                "The Schema Registry URL must use http:// or https:// without credentials, query or fragment. Enter credentials separately.", nameof(profile.Kafka)));
        }
        if (settings.SchemaRegistryUserName is not null && settings.SchemaRegistryUrl is null)
        {
            errors.Add(new ValidationError("profile.kafka.schema_registry.user_without_url",
                "Enter the Schema Registry URL for its user name.", nameof(profile.Kafka)));
        }
    }

    [GeneratedRegex("^[a-z]{2}(-[a-z]+)+-[0-9]+$")]
    private static partial Regex AwsRegionPattern();

    // Project IDs are 6-30 lower-case characters; domain-scoped projects add "example.com:".
    [GeneratedRegex("^([a-z0-9.-]+:)?[a-z][a-z0-9-]{4,28}[a-z0-9]$")]
    private static partial Regex GoogleProjectPattern();

    [GeneratedRegex("^[A-Za-z0-9.-]+:[0-9]{1,5}$")]
    private static partial Regex HostAndPortPattern();

    private static void ValidateAuthentication(
        ServiceBusProfile profile,
        ICollection<ValidationError> errors)
    {
        if (profile.Authentication is null)
        {
            errors.Add(new ValidationError(
                "profile.authentication.required",
                "An authentication method is required.",
                nameof(profile.Authentication)));
            return;
        }

        if (!Enum.IsDefined(profile.Authentication.Kind))
        {
            errors.Add(new ValidationError(
                "profile.authentication.invalid",
                "The authentication method is not supported.",
                nameof(profile.Authentication)));
            return;
        }

        if (profile.Authentication.Kind == AuthenticationKind.ConnectionString &&
            profile.Authentication.EntraId is not null)
        {
            errors.Add(new ValidationError(
                "profile.authentication.entra_unexpected",
                "Entra ID settings cannot be used with connection string authentication.",
                nameof(profile.Authentication)));
        }

        if (profile.Authentication.Kind == AuthenticationKind.EntraId &&
            profile.Authentication.EntraId is null)
        {
            errors.Add(new ValidationError(
                "profile.authentication.entra_required",
                "Entra ID settings are required for Entra ID authentication.",
                nameof(profile.Authentication)));
        }
    }

    private static void ValidateNamespace(
        ServiceBusProfile profile,
        ICollection<ValidationError> errors)
    {
        var namespaceValue = profile.FullyQualifiedNamespace;
        if (string.IsNullOrWhiteSpace(namespaceValue))
        {
            if (profile.Authentication?.Kind == AuthenticationKind.EntraId)
            {
                errors.Add(new ValidationError(
                    "profile.namespace.required",
                    "A fully qualified namespace is required for Entra ID authentication.",
                    nameof(profile.FullyQualifiedNamespace)));
            }

            return;
        }

        var value = namespaceValue.Trim();
        if (value.Contains("://", StringComparison.Ordinal) ||
            value.Contains('/') ||
            value.Contains('\\') ||
            value.Contains('?') ||
            value.Contains('#') ||
            value.Contains(':') ||
            Uri.CheckHostName(value) != UriHostNameType.Dns)
        {
            errors.Add(new ValidationError(
                "profile.namespace.invalid",
                "Use a bare fully qualified namespace, for example contoso.servicebus.windows.net.",
                nameof(profile.FullyQualifiedNamespace)));
        }
    }
}
