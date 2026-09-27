namespace QueueLoom.Core.Profiles;

/// <summary>
/// A non-secret, persistable description of a messaging environment: an Azure Service Bus namespace,
/// an AWS account region (SQS and SNS) or a Google Cloud project (Pub/Sub).
/// </summary>
public sealed record ServiceBusProfile(
    Guid Id,
    string Name,
    EnvironmentKind Environment,
    string? CustomEnvironmentName,
    string? FullyQualifiedNamespace,
    AuthenticationSettings Authentication,
    ProfileAccessMode AccessMode = ProfileAccessMode.ReadOnly)
{
    public int EmulatorManagementPort { get; init; } = 5300;

    /// <summary>Profiles saved before other clouds were supported have no provider and are Azure Service Bus.</summary>
    public MessagingProvider Provider { get; init; } = MessagingProvider.AzureServiceBus;

    public AwsSettings? Aws { get; init; }

    public GooglePubSubSettings? GooglePubSub { get; init; }

    /// <summary>Where the environment lives, in the provider's own terms: namespace, region or project.</summary>
    public string? EndpointDisplay => Provider switch
    {
        MessagingProvider.AmazonSqsSns when Aws is not null => string.IsNullOrWhiteSpace(Aws.ServiceUrl)
            ? Aws.Region
            : $"{Aws.Region} · {Aws.ServiceUrl}",
        MessagingProvider.GooglePubSub when GooglePubSub is not null => string.IsNullOrWhiteSpace(GooglePubSub.EmulatorHost)
            ? GooglePubSub.ProjectId
            : $"{GooglePubSub.ProjectId} · emulator {GooglePubSub.EmulatorHost}",
        _ => FullyQualifiedNamespace
    };

    public string EnvironmentDisplayName => Environment switch
    {
        EnvironmentKind.Development => "Development",
        EnvironmentKind.Test => "Test",
        EnvironmentKind.Production => "Production",
        EnvironmentKind.Custom => string.IsNullOrWhiteSpace(CustomEnvironmentName)
            ? "Custom"
            : CustomEnvironmentName.Trim(),
        _ => Environment.ToString()
    };

    public string AuthenticationDisplayName => Authentication.Kind switch
    {
        AuthenticationKind.ConnectionString => "SAS connection string",
        AuthenticationKind.EntraId => $"Entra ID · {Authentication.EntraId?.CredentialKind}",
        AuthenticationKind.AwsAccessKey => "AWS access key",
        AuthenticationKind.AwsDefaultCredentials => string.IsNullOrWhiteSpace(Aws?.ProfileName)
            ? "AWS default credentials"
            : $"AWS profile · {Aws.ProfileName}",
        AuthenticationKind.GoogleServiceAccountKey => "Service account key",
        AuthenticationKind.GoogleApplicationDefault => string.IsNullOrWhiteSpace(GooglePubSub?.EmulatorHost)
            ? "Application Default Credentials"
            : "Emulator · no credentials",
        _ => Authentication.Kind.ToString()
    };

    public bool CanWrite => AccessMode == ProfileAccessMode.ReadWrite;

    public static ServiceBusProfile CreateNew(
        string name,
        EnvironmentKind environment,
        AuthenticationSettings authentication,
        string? fullyQualifiedNamespace = null,
        string? customEnvironmentName = null,
        ProfileAccessMode accessMode = ProfileAccessMode.ReadOnly) =>
        new(
            Guid.NewGuid(),
            name,
            environment,
            customEnvironmentName,
            fullyQualifiedNamespace,
            authentication,
            accessMode);
}
