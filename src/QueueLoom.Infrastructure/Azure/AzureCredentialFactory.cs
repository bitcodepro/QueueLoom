using System.Net;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Identity;
using QueueLoom.Core.Profiles;

namespace QueueLoom.Infrastructure.Azure;

internal static class AzureCredentialFactory
{
    public static TokenCredential Create(EntraIdSettings settings, IWebProxy? proxy = null)
    {
        ArgumentNullException.ThrowIfNull(settings);

        var transport = AzureProxy.Transport(proxy);
        return settings.CredentialKind switch
        {
            EntraIdCredentialKind.DefaultAzureCredential => CreateDefault(settings, transport),
            EntraIdCredentialKind.InteractiveBrowser => CreateInteractive(settings, transport),
            EntraIdCredentialKind.AzureCli => CreateAzureCli(settings),
            EntraIdCredentialKind.ManagedIdentity => CreateManagedIdentity(settings, transport),
            _ => throw new ArgumentOutOfRangeException(
                nameof(settings),
                settings.CredentialKind,
                "Unsupported Entra ID credential type.")
        };
    }

    private static DefaultAzureCredential CreateDefault(EntraIdSettings settings, HttpPipelineTransport? transport)
    {
        var options = new DefaultAzureCredentialOptions
        {
            ExcludeInteractiveBrowserCredential = true
        };
        UseTransport(options, transport);
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

    private static InteractiveBrowserCredential CreateInteractive(EntraIdSettings settings, HttpPipelineTransport? transport)
    {
        // Keep interactive tokens in memory only. This makes deleting an
        // environment complete and avoids leaving a second credential store
        // that QueueLoom cannot reliably enumerate or erase cross-platform.
        var options = new InteractiveBrowserCredentialOptions();
        UseTransport(options, transport);
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

    private static ManagedIdentityCredential CreateManagedIdentity(EntraIdSettings settings, HttpPipelineTransport? transport)
    {
        var options = new ManagedIdentityCredentialOptions(string.IsNullOrWhiteSpace(settings.ClientId)
            ? ManagedIdentityId.SystemAssigned
            : ManagedIdentityId.FromUserAssignedClientId(settings.ClientId.Trim()));
        UseTransport(options, transport);
        return new ManagedIdentityCredential(options);
    }

    // Token requests go through the environment's proxy too; the Azure CLI runs as its own process with its own proxy.
    private static void UseTransport(TokenCredentialOptions options, HttpPipelineTransport? transport)
    {
        if (transport is not null)
        {
            options.Transport = transport;
        }
    }
}
