namespace QueueLoom.Core.Profiles;

/// <summary>
/// A non-secret, persistable description of an Azure Service Bus environment.
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
