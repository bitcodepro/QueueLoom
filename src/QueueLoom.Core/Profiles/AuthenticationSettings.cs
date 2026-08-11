namespace QueueLoom.Core.Profiles;

/// <summary>
/// Describes how a profile authenticates without containing any secret material.
/// Connection strings are kept separately by <c>ISecretVault</c>.
/// </summary>
public sealed record AuthenticationSettings(
    AuthenticationKind Kind,
    EntraIdSettings? EntraId = null)
{
    public static AuthenticationSettings ConnectionString() =>
        new(AuthenticationKind.ConnectionString);

    public static AuthenticationSettings Entra(
        EntraIdCredentialKind credentialKind = EntraIdCredentialKind.DefaultAzureCredential,
        string? tenantId = null,
        string? clientId = null) =>
        new(AuthenticationKind.EntraId, new EntraIdSettings(credentialKind, tenantId, clientId));
}
