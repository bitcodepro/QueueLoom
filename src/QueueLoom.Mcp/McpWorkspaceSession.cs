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
        return all.FirstOrDefault(profile => string.Equals(profile.Name, key, StringComparison.OrdinalIgnoreCase))
               ?? all.FirstOrDefault(profile => Guid.TryParse(key, out var id) && profile.Id == id)
               ?? throw new McpException(
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
                    logger.LogWarning(exception, "Could not return the MCP connection to read-only; disconnecting");
                    await workspace.DisconnectAsync(CancellationToken.None).ConfigureAwait(false);
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
        if (workspace.ConnectionState == WorkspaceConnectionState.Connected && workspace.ConnectedProfileId == profile.Id)
        {
            return;
        }

        await workspace.ConnectAsync(profile with { AccessMode = ProfileAccessMode.ReadOnly }, cancellationToken)
            .ConfigureAwait(false);
    }
}
