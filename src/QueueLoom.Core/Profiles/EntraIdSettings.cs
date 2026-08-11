namespace QueueLoom.Core.Profiles;

public sealed record EntraIdSettings(
    EntraIdCredentialKind CredentialKind = EntraIdCredentialKind.DefaultAzureCredential,
    string? TenantId = null,
    string? ClientId = null);
