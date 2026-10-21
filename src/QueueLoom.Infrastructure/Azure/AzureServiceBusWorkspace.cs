using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using Azure;
using Azure.Core;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.Routing;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using QueueLoom.Infrastructure.Persistence;

namespace QueueLoom.Infrastructure.Azure;

public sealed partial class AzureServiceBusWorkspace : IServiceBusWorkspace, IWorkspaceNoticeSource
{
    /// <inheritdoc />
    public event EventHandler<WorkspaceNotice>? Notice;

    /// <summary>Announces a notice without allowing a listener to replace the operation's result.</summary>
    private void ReportNotice(string title, string details)
    {
        try
        {
            Notice?.Invoke(this, new WorkspaceNotice(title, details));
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            // A listener's failure must not turn the operation's result into an error.
        }
    }

    private static readonly TimeSpan TopologyCacheDuration = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan InteractiveTryTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan InteractiveRetryDelay = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan InteractiveMaximumRetryDelay = TimeSpan.FromSeconds(3);
    private static readonly TimeSpan PurgeReceiveWaitTime = TimeSpan.FromMilliseconds(250);
    private static readonly TimeSpan PurgeVerificationTimeout = TimeSpan.FromSeconds(2);
    private const int MonitorConcurrency = 6;
    private const int SearchConcurrency = 12;
    private const int BrowseBatchSize = 250;
    private const int InteractiveMaximumRetries = 2;
    private const int PurgeEmptyReceiveConfirmations = 2;
    private const int BackupWriteConcurrency = 4;
    private static readonly TimeSpan SessionAcceptWait = TimeSpan.FromSeconds(2);
    private const int MaximumSessionsPerBrowse = 100;

    private readonly ISecretVault _secretVault;
    private readonly DeadLetterJsonBackupStore _backupStore;
    private readonly TimeProvider _timeProvider;
    private readonly AsyncOperationGate _operationGate = new();
    private readonly SemaphoreSlim _topologyGate = new(1, 1);
    private readonly ConcurrentDictionary<string, ServiceBusSender> _senders = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DeadLetterMeasurement?> _previousDeadLetterCounts = new(StringComparer.Ordinal);

    private ServiceBusClient? _client;
    private ServiceBusAdministrationClient? _administration;
    private ServiceBusProfile? _profile;
    private ServiceBusTopology? _cachedTopology;
    private WorkspaceConnectionState _connectionState;
    private bool _disposed;

    public AzureServiceBusWorkspace(
        ISecretVault secretVault,
        TimeProvider? timeProvider = null,
        DeadLetterJsonBackupStore? backupStore = null)
    {
        ArgumentNullException.ThrowIfNull(secretVault);
        _secretVault = secretVault;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _backupStore = backupStore ?? new DeadLetterJsonBackupStore(QueueLoomPaths.CreateDefault());
    }

    public WorkspaceConnectionState ConnectionState => _connectionState;

    public Guid? ConnectedProfileId => _profile?.Id;
    public string? ConnectedNamespace => _profile?.FullyQualifiedNamespace;
    public string? ConnectedConfigurationIdentity => _profile is null ? null : ScheduledResend.IdentityFor(_profile);
    public MessagingProvider? ConnectedProvider => _profile?.Provider;

    public async Task ConnectAsync(
        ServiceBusProfile profile,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(profile);
        ThrowIfDisposed();

        using (await _operationGate.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false))
        {
            ThrowIfDisposed();
            _connectionState = WorkspaceConnectionState.Connecting;
            await DisposeClientsAsync().ConfigureAwait(false);

            ServiceBusClient? client = null;
            ServiceBusAdministrationClient? administration = null;
            var isEmulator = false;
            try
            {
                var clientOptions = CreateMessagingClientOptions(profile.UseAmqpWebSockets);
                var administrationOptions = CreateAdministrationClientOptions();

                if (profile.Authentication.Kind == AuthenticationKind.ConnectionString)
                {
                    var connectionString = await _secretVault.RetrieveForProfileAsync(
                        profile, ProfileSecretKind.ConnectionString,
                        cancellationToken).ConfigureAwait(false);
                    if (string.IsNullOrWhiteSpace(connectionString))
                    {
                        throw new InvalidOperationException("This environment has no saved connection string.");
                    }

                    var parsed = ServiceBusConnectionStringProperties.Parse(connectionString);
                    isEmulator = EmulatorConnection.IsEmulator(connectionString);
                    if (isEmulator && profile.UseAmqpWebSockets)
                    {
                        throw new InvalidOperationException(
                            "The Service Bus emulator accepts AMQP over TCP only. Turn off AMQP over WebSockets for this environment.");
                    }
                    if (!string.IsNullOrWhiteSpace(parsed.EntityPath))
                    {
                        throw new InvalidOperationException(
                            "QueueLoom requires a namespace-level connection string without EntityPath to list all queues and topics.");
                    }

                    // Older SAS profiles may have no namespace metadata. When one is declared,
                    // never display it as the target of clients constructed for a different endpoint.
                    if (!string.IsNullOrWhiteSpace(profile.FullyQualifiedNamespace) &&
                        !string.Equals(ProfileValidator.NormalizeFullyQualifiedNamespace(profile.FullyQualifiedNamespace),
                            parsed.FullyQualifiedNamespace, StringComparison.OrdinalIgnoreCase))
                    {
                        throw new InvalidOperationException(
                            "The saved connection string namespace does not match this environment. Edit the environment and re-enter its connection string.");
                    }

                    client = new ServiceBusClient(connectionString, clientOptions);
                    administration = new ServiceBusAdministrationClient(
                        EmulatorConnection.AdministrationConnectionString(connectionString, profile.EmulatorManagementPort), administrationOptions);
                }
                else
                {
                    if (string.IsNullOrWhiteSpace(profile.FullyQualifiedNamespace))
                    {
                        throw new InvalidOperationException("A fully qualified Service Bus namespace is required for Entra ID.");
                    }

                    var settings = profile.Authentication.EntraId
                        ?? throw new InvalidOperationException("Entra ID settings are missing.");
                    TokenCredential credential = AzureCredentialFactory.Create(settings);
                    client = new ServiceBusClient(profile.FullyQualifiedNamespace, credential, clientOptions);
                    administration = new ServiceBusAdministrationClient(
                        profile.FullyQualifiedNamespace,
                        credential,
                        administrationOptions);
                }

                // This validates both the endpoint and the Manage/Data Owner permission needed
                // for topology discovery, without receiving or locking any messages.
                await administration.GetNamespacePropertiesAsync(cancellationToken).ConfigureAwait(false);

                _client = client;
                _isEmulator = isEmulator;
                _administration = administration;
                _profile = profile;
                _cachedTopology = null;
                _previousDeadLetterCounts.Clear();
                _connectionState = WorkspaceConnectionState.Connected;
            }
            catch
            {
                if (client is not null)
                {
                    await client.DisposeAsync().ConfigureAwait(false);
                }
                _connectionState = WorkspaceConnectionState.Faulted;
                throw;
            }
        }
    }

    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        using (await _operationGate.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false))
        {
            ThrowIfDisposed();
            await DisposeClientsAsync().ConfigureAwait(false);
            _connectionState = WorkspaceConnectionState.Disconnected;
        }
    }

    public Task SetAccessModeAsync(
        ProfileAccessMode accessMode,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        var profile = GetConnectedProfile();
        _profile = profile with { AccessMode = accessMode };
        return Task.CompletedTask;
    }

    public async Task<ServiceBusTopology> GetTopologyAsync(
        bool forceRefresh = false,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        ThrowIfDisposed();
        return await GetTopologyCoreAsync(forceRefresh, cancellationToken).ConfigureAwait(false);
    }

    private async Task<ServiceBusTopology> GetTopologyCoreAsync(
        bool forceRefresh,
        CancellationToken cancellationToken)
    {
        var cached = _cachedTopology;
        if (!forceRefresh && cached is not null &&
            _timeProvider.GetUtcNow() - cached.FetchedAt < TopologyCacheDuration)
        {
            return cached;
        }

        await _topologyGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            cached = _cachedTopology;
            if (!forceRefresh && cached is not null &&
                _timeProvider.GetUtcNow() - cached.FetchedAt < TopologyCacheDuration)
            {
                return cached;
            }

            var administration = GetAdministrationClient();
            var queuePropertiesTask = ReadAllAsync(administration.GetQueuesAsync(cancellationToken), cancellationToken);
            var queueRuntimeTask = ReadAllAsync(administration.GetQueuesRuntimePropertiesAsync(cancellationToken), cancellationToken);
            var topicPropertiesTask = ReadAllAsync(administration.GetTopicsAsync(cancellationToken), cancellationToken);
            var topicRuntimeTask = ReadAllAsync(administration.GetTopicsRuntimePropertiesAsync(cancellationToken), cancellationToken);

            await Task.WhenAll(queuePropertiesTask, queueRuntimeTask, topicPropertiesTask, topicRuntimeTask)
                .ConfigureAwait(false);

            var queueRuntime = queueRuntimeTask.Result.ToDictionary(item => item.Name, StringComparer.Ordinal);
            var queues = queuePropertiesTask.Result
                .Select(properties => MapQueue(properties, queueRuntime.GetValueOrDefault(properties.Name)))
                .OrderBy(queue => queue.Name, StringComparer.OrdinalIgnoreCase)
                .ToArray();

            var topicRuntime = topicRuntimeTask.Result.ToDictionary(item => item.Name, StringComparer.Ordinal);
            using var limiter = new SemaphoreSlim(MonitorConcurrency, MonitorConcurrency);
            var topicTasks = topicPropertiesTask.Result.Select(async topic =>
            {
                await limiter.WaitAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    return await MapTopicAsync(
                        administration,
                        topic,
                        topicRuntime.GetValueOrDefault(topic.Name),
                        cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    limiter.Release();
                }
            });

            // A topic deleted while the refresh reads its subscriptions is left out (see MapTopicAsync).
            var topics = (await Task.WhenAll(topicTasks).ConfigureAwait(false))
                .OfType<ServiceBusTopic>()
                .OrderBy(topic => topic.Name, StringComparer.OrdinalIgnoreCase)
                .ToArray();

            var topology = new ServiceBusTopology(_timeProvider.GetUtcNow(), queues, topics);
            // Forwarding notes come last: they describe where messages go, whatever the counts.
            return _cachedTopology = Forwarding.Annotate(
                _isEmulator ? await SampleEmulatorTopologyAsync(topology, cancellationToken).ConfigureAwait(false) : topology);
        }
        finally
        {
            _topologyGate.Release();
        }
    }

    /// <param name="webSockets">AMQP over WebSockets (port 443) instead of AMQP over TCP (port 5671).</param>
    internal static ServiceBusClientOptions CreateMessagingClientOptions(bool webSockets = false) => new()
    {
        TransportType = webSockets ? ServiceBusTransportType.AmqpWebSockets : ServiceBusTransportType.AmqpTcp,
        EnableCrossEntityTransactions = false,
        RetryOptions = new ServiceBusRetryOptions
        {
            Mode = ServiceBusRetryMode.Exponential,
            MaxRetries = InteractiveMaximumRetries,
            Delay = InteractiveRetryDelay,
            MaxDelay = InteractiveMaximumRetryDelay,
            TryTimeout = InteractiveTryTimeout
        }
    };

    internal static ServiceBusAdministrationClientOptions CreateAdministrationClientOptions()
    {
        var options = new ServiceBusAdministrationClientOptions();
        options.Retry.Mode = RetryMode.Exponential;
        options.Retry.MaxRetries = InteractiveMaximumRetries;
        options.Retry.Delay = InteractiveRetryDelay;
        options.Retry.MaxDelay = InteractiveMaximumRetryDelay;
        options.Retry.NetworkTimeout = InteractiveTryTimeout;
        return options;
    }

    private void EnsureWriteAllowed()
    {
        var profile = GetConnectedProfile();
        if (!profile.CanWrite)
        {
            throw new InvalidOperationException(
                $"Environment '{profile.Name}' is read-only. Unlock write access before sending messages.");
        }
    }

    private ServiceBusProfile GetConnectedProfile() =>
        _profile ?? throw new InvalidOperationException("Connect to an environment first.");

    private ServiceBusClient GetMessagingClient() =>
        _client ?? throw new InvalidOperationException("Connect to an environment first.");

    private ServiceBusAdministrationClient GetAdministrationClient() =>
        _administration ?? throw new InvalidOperationException("Connect to an environment first.");

    private async Task DisposeClientsAsync()
    {
        // Release everything even when one link fails to close; a faulted sender must not leak the client.
        var senders = _senders.Values.ToArray();
        _senders.Clear();
        var client = _client;
        _client = null;
        _administration = null;
        _profile = null;
        _cachedTopology = null;
        _previousDeadLetterCounts.Clear();

        foreach (var sender in senders)
        {
            try
            {
                await sender.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Closing a link that already failed is not actionable; disconnect, reconnect and dispose must go on.
            }
        }

        if (client is not null)
        {
            try
            {
                await client.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Closing a link that already failed is not actionable; disconnect, reconnect and dispose must go on.
            }
        }

    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        using var lifecycle = await _operationGate.EnterLifecycleAsync().ConfigureAwait(false);
        if (_disposed)
        {
            return;
        }

        await DisposeClientsAsync().ConfigureAwait(false);
        _connectionState = WorkspaceConnectionState.Disconnected;
        _disposed = true;
        _topologyGate.Dispose();
    }
}
