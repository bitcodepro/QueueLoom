using System.Net;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;

namespace QueueLoom.Infrastructure.Messaging;

/// <summary>Turns an environment's proxy setting into the proxy its HTTP and WebSocket clients use.</summary>
internal static class ProxyFactory
{
    /// <summary>The proxy for the environment, or null to leave .NET's own (the computer's proxy) in place.</summary>
    public static async Task<IWebProxy?> CreateAsync(ServiceBusProfile profile, ISecretVault vault, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(profile);
        ArgumentNullException.ThrowIfNull(vault);
        var settings = profile.Proxy;
        var password = settings is { Mode: ProxyMode.Custom, UserName: not null }
            ? await vault.RetrieveForProfileAsync(profile, ProfileSecretKind.ProxyPassword, cancellationToken).ConfigureAwait(false)
            : null;
        return Create(settings, password);
    }

    public static IWebProxy? Create(ProxySettings? settings, string? password) => settings?.Mode switch
    {
        ProxyMode.None => new WebProxy(),
        ProxyMode.Custom => new WebProxy(new Uri(settings.Address!.Trim()))
        {
            // Managed identity's metadata endpoint and this computer (emulators) are always reached directly.
            BypassList = [@"^https?://169\.254\.169\.254(:\d+)?$", @"^https?://(localhost|127\.\d+\.\d+\.\d+|\[::1\])(:\d+)?$"],
            BypassProxyOnLocal = false,
            Credentials = string.IsNullOrWhiteSpace(settings.UserName)
                ? null
                : new NetworkCredential(settings.UserName.Trim(), password ?? string.Empty)
        },
        _ => null
    };

    /// <summary>Gives a handler the environment's proxy; with none it keeps the computer's.</summary>
    public static void Apply(SocketsHttpHandler handler, IWebProxy? proxy)
    {
        ArgumentNullException.ThrowIfNull(handler);
        if (proxy is not null)
        {
            handler.Proxy = proxy;
            handler.UseProxy = true;
        }
    }
}
