using QueueLoom.Core.Routing;
using QueueLoom.Core.Abstractions;
using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Messaging;

/// <summary>
/// The workspace the app talks to. It forwards every call to the workspace of the connected environment's
/// provider (Azure Service Bus, Amazon SQS / SNS, Google Pub/Sub, RabbitMQ or Kafka), so pages and the MCP server stay
/// provider-agnostic.
/// </summary>
public sealed class MultiProviderWorkspace : IServiceBusWorkspace
{
    private readonly Func<MessagingProvider, IServiceBusWorkspace> _factory;
    private readonly Dictionary<MessagingProvider, IServiceBusWorkspace> _workspaces = [];
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IServiceBusWorkspace? _current;
    private bool _disposed;

    public MultiProviderWorkspace(Func<MessagingProvider, IServiceBusWorkspace> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        _factory = factory;
    }

    public WorkspaceConnectionState ConnectionState => _current?.ConnectionState ?? WorkspaceConnectionState.Disconnected;

    public Guid? ConnectedProfileId => _current?.ConnectedProfileId;

    public string? ConnectedNamespace => _current?.ConnectedNamespace;

    /// <summary>The provider of the environment the workspace is connected (or connecting) to.</summary>
    public MessagingProvider? ConnectedProvider { get; private set; }

    public async Task ConnectAsync(ServiceBusProfile profile, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(profile);
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!_workspaces.TryGetValue(profile.Provider, out var target))
            {
                target = _factory(profile.Provider);
                _workspaces[profile.Provider] = target;
            }

            // Only one environment is connected at a time, whichever cloud it is in.
            if (_current is not null && !ReferenceEquals(_current, target) &&
                _current.ConnectionState != WorkspaceConnectionState.Disconnected)
            {
                await _current.DisconnectAsync(cancellationToken).ConfigureAwait(false);
            }

            _current = target;
            ConnectedProvider = profile.Provider;
            await target.ConnectAsync(profile, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    public Task DisconnectAsync(CancellationToken cancellationToken = default) =>
        _current?.DisconnectAsync(cancellationToken) ?? Task.CompletedTask;

    public Task SetAccessModeAsync(ProfileAccessMode accessMode, CancellationToken cancellationToken = default) =>
        Current.SetAccessModeAsync(accessMode, cancellationToken);

    public Task<ServiceBusTopology> GetTopologyAsync(bool forceRefresh = false, CancellationToken cancellationToken = default) =>
        Current.GetTopologyAsync(forceRefresh, cancellationToken);

    public Task<IReadOnlyList<BrowsedMessage>> BrowseMessagesAsync(
        BrowseMessagesRequest request,
        CancellationToken cancellationToken = default) =>
        Current.BrowseMessagesAsync(request, cancellationToken);

    public Task<DeadLetterSearchResult> SearchDeadLettersAsync(
        DeadLetterSearchRequest request,
        CancellationToken cancellationToken = default) =>
        Current.SearchDeadLettersAsync(request, cancellationToken);

    public Task SendMessageAsync(SendMessageRequest request, CancellationToken cancellationToken = default) =>
        Current.SendMessageAsync(request, cancellationToken);

    public Task ResubmitDeadLetterAsync(ResubmitDeadLetterRequest request, CancellationToken cancellationToken = default) =>
        Current.ResubmitDeadLetterAsync(request, cancellationToken);

    public Task<DeadLetterPurgeResult> PurgeDeadLettersAsync(
        DeadLetterPurgeRequest request,
        CancellationToken cancellationToken = default,
        IProgress<DeadLetterPurgeProgress>? progress = null) =>
        Current.PurgeDeadLettersAsync(request, cancellationToken, progress);

    public Task<DeleteDeadLetterMessagesResult> DeleteDeadLetterMessagesAsync(
        DeleteDeadLetterMessagesRequest request,
        CancellationToken cancellationToken = default,
        IProgress<DeadLetterMessageDeletionProgress>? progress = null) =>
        Current.DeleteDeadLetterMessagesAsync(request, cancellationToken, progress);

    public QueueManagementCapabilities? QueueManagement => _current?.QueueManagement;

    public Task<QueueSettings> GetQueueSettingsAsync(string queue, CancellationToken cancellationToken = default) =>
        Current.GetQueueSettingsAsync(queue, cancellationToken);

    public Task CreateQueueAsync(QueueDefinition definition, CancellationToken cancellationToken = default) =>
        Current.CreateQueueAsync(definition, cancellationToken);

    public Task UpdateQueueSettingsAsync(string queue, QueueSettings settings, CancellationToken cancellationToken = default) =>
        Current.UpdateQueueSettingsAsync(queue, settings, cancellationToken);

    public Task DeleteQueueAsync(string queue, CancellationToken cancellationToken = default) =>
        Current.DeleteQueueAsync(queue, cancellationToken);

    public bool SupportsSubscriptionRules => _current?.SupportsSubscriptionRules == true;

    public Task<IReadOnlyList<SubscriptionRules>> GetTopicRulesAsync(string topic, CancellationToken cancellationToken = default) =>
        Current.GetTopicRulesAsync(topic, cancellationToken);

    public Task SaveSubscriptionRuleAsync(string topic, string subscription, SubscriptionRule rule, bool replace,
        CancellationToken cancellationToken = default) =>
        Current.SaveSubscriptionRuleAsync(topic, subscription, rule, replace, cancellationToken);

    public Task DeleteSubscriptionRuleAsync(string topic, string subscription, string rule, CancellationToken cancellationToken = default) =>
        Current.DeleteSubscriptionRuleAsync(topic, subscription, rule, cancellationToken);

    public Task<RemovePendingMessagesResult> RemovePendingMessagesAsync(
        IReadOnlyList<BrowsedMessage> messages,
        CancellationToken cancellationToken = default) =>
        Current.RemovePendingMessagesAsync(messages, cancellationToken);

    public Task<DeadLetterSnapshot> GetDeadLetterSnapshotAsync(
        DeadLetterMonitorScope scope,
        CancellationToken cancellationToken = default) =>
        Current.GetDeadLetterSnapshotAsync(scope, cancellationToken);

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        foreach (var workspace in _workspaces.Values)
        {
            await workspace.DisposeAsync().ConfigureAwait(false);
        }
        _workspaces.Clear();
        _gate.Dispose();
    }

    private IServiceBusWorkspace Current
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _current ?? throw new InvalidOperationException("Connect to an environment first.");
        }
    }
}

public static class MessagingWorkspaces
{
    /// <summary>The workspace for every supported cloud, sharing one secret vault and one backup folder.</summary>
    public static MultiProviderWorkspace Create(ISecretVault secretVault, Persistence.QueueLoomPaths paths)
    {
        ArgumentNullException.ThrowIfNull(secretVault);
        ArgumentNullException.ThrowIfNull(paths);
        var backupStore = new Persistence.DeadLetterJsonBackupStore(paths);
        return new MultiProviderWorkspace(provider => provider switch
        {
            MessagingProvider.AmazonSqsSns => new Aws.AwsSqsSnsWorkspace(secretVault, backupStore: backupStore),
            MessagingProvider.GooglePubSub => new Google.GooglePubSubWorkspace(secretVault, backupStore: backupStore),
            MessagingProvider.RabbitMq => new RabbitMq.RabbitMqWorkspace(secretVault, backupStore: backupStore),
            MessagingProvider.Kafka => new Kafka.KafkaWorkspace(secretVault, backupStore: backupStore),
            _ => new Azure.AzureServiceBusWorkspace(secretVault, backupStore: backupStore)
        });
    }
}
