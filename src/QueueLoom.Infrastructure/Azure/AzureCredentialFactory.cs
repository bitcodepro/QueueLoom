using Azure.Core;
using Azure.Identity;
using QueueLoom.Core.Profiles;

namespace QueueLoom.Infrastructure.Azure;

internal static class AzureCredentialFactory
{
    public static TokenCredential Create(EntraIdSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);

        return settings.CredentialKind switch
        {
            EntraIdCredentialKind.DefaultAzureCredential => CreateDefault(settings),
            EntraIdCredentialKind.InteractiveBrowser => CreateInteractive(settings),
            EntraIdCredentialKind.AzureCli => CreateAzureCli(settings),
            EntraIdCredentialKind.ManagedIdentity => CreateManagedIdentity(settings),
            _ => throw new ArgumentOutOfRangeException(
                nameof(settings),
                settings.CredentialKind,
                "Unsupported Entra ID credential type.")
        };
    }

    private static DefaultAzureCredential CreateDefault(EntraIdSettings settings)
    {
        var options = new DefaultAzureCredentialOptions
        {
            ExcludeInteractiveBrowserCredential = true
        };
        if (!string.IsNullOrWhiteSpace(settings.TenantId))
        {
            options.TenantId = settings.TenantId.Trim();
        }
        if (!string.IsNullOrWhiteSpace(settings.ClientId))
        {
            options.ManagedIdentityClientId = settings.ClientId.Trim();
        }

        return new DefaultAzureCredential(options);
    }

    private static InteractiveBrowserCredential CreateInteractive(EntraIdSettings settings)
    {
        // Keep interactive tokens in memory only. This makes deleting an
        // environment complete and avoids leaving a second credential store
        // that QueueLoom cannot reliably enumerate or erase cross-platform.
        var options = new InteractiveBrowserCredentialOptions();
        if (!string.IsNullOrWhiteSpace(settings.TenantId))
        {
            options.TenantId = settings.TenantId.Trim();
        }
        if (!string.IsNullOrWhiteSpace(settings.ClientId))
        {
            options.ClientId = settings.ClientId.Trim();
        }

        return new InteractiveBrowserCredential(options);
    }

    private static AzureCliCredential CreateAzureCli(EntraIdSettings settings)
    {
        var options = new AzureCliCredentialOptions();
        if (!string.IsNullOrWhiteSpace(settings.TenantId))
        {
            options.TenantId = settings.TenantId.Trim();
        }

        return new AzureCliCredential(options);
    }

    private static ManagedIdentityCredential CreateManagedIdentity(EntraIdSettings settings) =>
        string.IsNullOrWhiteSpace(settings.ClientId)
            ? new ManagedIdentityCredential(ManagedIdentityId.SystemAssigned)
            : new ManagedIdentityCredential(ManagedIdentityId.FromUserAssignedClientId(settings.ClientId.Trim()));
}
