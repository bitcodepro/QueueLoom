using Microsoft.Extensions.Logging;
using ModelContextProtocol;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Mcp;

/// <summary>
/// Owns the Service Bus connection used by MCP tools. Tools run one at a time, reads always use a
/// read-only connection, and write access is granted only for the duration of an approved change.
/// </summary>
public sealed class McpWorkspaceSession(
    IProfileRepository profiles,
    IServiceBusWorkspace workspace,
    ILogger<McpWorkspaceSession> logger,
    IActivityJournal? journal = null) : IDisposable
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private string? _connectedConfigurationIdentity;

    public IServiceBusWorkspace Workspace => workspace;

    public Task<IReadOnlyList<ServiceBusProfile>> ListProfilesAsync(CancellationToken cancellationToken) =>
        profiles.ListAsync(cancellationToken);

    /// <summary>Finds a saved environment by name or ID; with a single saved environment the argument is optional.</summary>
    public async Task<ServiceBusProfile> ResolveProfileAsync(string? environment, CancellationToken cancellationToken)
    {
        var all = await profiles.ListAsync(cancellationToken).ConfigureAwait(false);
        if (all.Count == 0)
        {
            throw new McpException("No environments are saved. Add one in the QueueLoom app first.");
        }
        if (string.IsNullOrWhiteSpace(environment))
        {
            return all.Count == 1
                ? all[0]
                : throw new McpException(
                    $"Several environments are saved; pass one of: {string.Join(", ", all.Select(profile => profile.Name))}.");
        }

        var key = environment.Trim();
        if (Guid.TryParse(key, out var id) && all.FirstOrDefault(profile => profile.Id == id) is { } byId)
        {
            return byId;
        }
        // An exact name wins over one that differs only in case; several left is a question, never a guess.
        foreach (var comparison in new[] { StringComparison.Ordinal, StringComparison.OrdinalIgnoreCase })
        {
            var matches = all.Where(profile => string.Equals(profile.Name, key, comparison)).ToArray();
            if (matches.Length == 1)
            {
                return matches[0];
            }
            if (matches.Length > 1)
            {
                throw new McpException(
                    $"Several environments are named '{key}'; pass the id of one: {string.Join(", ", matches.Select(profile => $"{profile.Name} ({profile.Id})"))}.");
            }
        }
        throw new McpException(
            $"Unknown environment '{key}'. Saved environments: {string.Join(", ", all.Select(profile => profile.Name))}.");
    }

    /// <summary>Runs a read against the environment over a read-only connection.</summary>
    public async Task<T> ReadAsync<T>(
        ServiceBusProfile profile,
        Func<IServiceBusWorkspace, CancellationToken, Task<T>> read,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await EnsureConnectedReadOnlyAsync(profile, cancellationToken).ConfigureAwait(false);
            return await read(workspace, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Runs an approved change with write access, then returns the connection to read-only.</summary>
    public async Task<T> WriteAsync<T>(
        ServiceBusProfile profile,
        Func<IServiceBusWorkspace, CancellationToken, Task<T>> write,
        CancellationToken cancellationToken)
    {
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await EnsureConnectedReadOnlyAsync(profile, cancellationToken).ConfigureAwait(false);
            await workspace.SetAccessModeAsync(ProfileAccessMode.ReadWrite, cancellationToken).ConfigureAwait(false);
            try
            {
                return await write(workspace, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                try
                {
                    await workspace.SetAccessModeAsync(ProfileAccessMode.ReadOnly, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    // The connection may still carry write access: the next tool call must not reuse it, whatever
                    // happens below. A failure here is logged, not thrown, so it cannot replace the write's own result.
                    _connectedConfigurationIdentity = null;
                    logger.LogWarning(exception, "Could not return the MCP connection to read-only; disconnecting");
                    try
                    {
                        await workspace.DisconnectAsync(CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception disconnect)
                    {
                        logger.LogError(disconnect,
                            "Could not disconnect the MCP connection either; the next tool call reconnects read-only");
                    }
                }
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    public void Record(string level, string action, string details, ServiceBusProfile profile, ServiceBusEntityReference? source = null)
    {
        logger.LogInformation("MCP {Action} in {Environment}: {Details}", action, profile.Name, details);
        try
        {
            journal?.Append(new ActivityRecord(
                Guid.NewGuid(), DateTimeOffset.UtcNow, level, $"MCP · {action}", details, profile.Id, profile.Name, source));
        }
        catch (Exception exception)
        {
            logger.LogWarning(exception, "The MCP activity record could not be written");
        }
    }

    public void Dispose() => _gate.Dispose();

    private async Task EnsureConnectedReadOnlyAsync(ServiceBusProfile profile, CancellationToken cancellationToken)
    {
        var identity = ScheduledResend.IdentityFor(profile);
        var saved = await profiles.GetAsync(profile.Id, cancellationToken).ConfigureAwait(false);
        if (saved is null || ScheduledResend.IdentityFor(saved) != identity)
        {
            throw new McpException("The environment configuration changed. Read it again and request a new approval before writing.");
        }
        if (workspace.ConnectionState == WorkspaceConnectionState.Connected && workspace.ConnectedProfileId == profile.Id &&
            _connectedConfigurationIdentity == identity &&
            (workspace.ConnectedConfigurationIdentity is null || workspace.ConnectedConfigurationIdentity == identity))
        {
            return;
        }

        await workspace.ConnectAsync(profile with { AccessMode = ProfileAccessMode.ReadOnly }, cancellationToken)
            .ConfigureAwait(false);
        _connectedConfigurationIdentity = identity;
        saved = await profiles.GetAsync(profile.Id, cancellationToken).ConfigureAwait(false);
        if (saved is null || ScheduledResend.IdentityFor(saved) != identity)
        {
            await workspace.DisconnectAsync(CancellationToken.None).ConfigureAwait(false);
            _connectedConfigurationIdentity = null;
            throw new McpException("The environment configuration changed while connecting. Review it and request a new approval.");
        }
    }
}
