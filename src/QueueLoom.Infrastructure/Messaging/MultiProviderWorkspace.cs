using QueueLoom.Infrastructure.Azure;
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
    private readonly AsyncOperationGate _operationGate = new();
    private IServiceBusWorkspace? _current;
    private volatile bool _disposed;
    private readonly TaskCompletionSource _disposalCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _disposeStarted;

    public MultiProviderWorkspace(Func<MessagingProvider, IServiceBusWorkspace> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        _factory = factory;
    }

    public WorkspaceConnectionState ConnectionState => _current?.ConnectionState ?? WorkspaceConnectionState.Disconnected;

    public Guid? ConnectedProfileId => _current?.ConnectedProfileId;

    public string? ConnectedNamespace => _current?.ConnectedNamespace;
    public string? ConnectedConfigurationIdentity => _current?.ConnectedConfigurationIdentity;

    /// <summary>The provider of the environment the workspace is connected (or connecting) to.</summary>
    public MessagingProvider? ConnectedProvider { get; private set; }

    public async Task ConnectAsync(ServiceBusProfile profile, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(profile);
        ObjectDisposedException.ThrowIf(_disposed, this);
        using var lifecycle = await _operationGate.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false);
        ObjectDisposedException.ThrowIf(_disposed, this);
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

    public async Task DisconnectAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        using var lifecycle = await _operationGate.EnterLifecycleAsync(cancellationToken).ConfigureAwait(false);
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_current is not null) await _current.DisconnectAsync(cancellationToken).ConfigureAwait(false);
    }

    public Task SetAccessModeAsync(ProfileAccessMode accessMode, CancellationToken cancellationToken = default) =>
        WithWorkspaceAsync(workspace => workspace.SetAccessModeAsync(accessMode, cancellationToken), cancellationToken);

    public Task<ServiceBusTopology> GetTopologyAsync(bool forceRefresh = false, CancellationToken cancellationToken = default) =>
        WithWorkspaceAsync(workspace => workspace.GetTopologyAsync(forceRefresh, cancellationToken), cancellationToken);

    public Task<IReadOnlyList<BrowsedMessage>> BrowseMessagesAsync(
        BrowseMessagesRequest request,
        CancellationToken cancellationToken = default) =>
        WithWorkspaceAsync(workspace => workspace.BrowseMessagesAsync(request, cancellationToken), cancellationToken);

    public Task<DeadLetterSearchResult> SearchDeadLettersAsync(
        DeadLetterSearchRequest request,
        CancellationToken cancellationToken = default) =>
        WithWorkspaceAsync(workspace => workspace.SearchDeadLettersAsync(request, cancellationToken), cancellationToken);

    public Task SendMessageAsync(SendMessageRequest request, CancellationToken cancellationToken = default) =>
        WithWorkspaceAsync(workspace => workspace.SendMessageAsync(request, cancellationToken), cancellationToken);

    public Task ResubmitDeadLetterAsync(ResubmitDeadLetterRequest request, CancellationToken cancellationToken = default) =>
        WithWorkspaceAsync(workspace => workspace.ResubmitDeadLetterAsync(request, cancellationToken), cancellationToken);

    public Task<DeadLetterPurgeResult> PurgeDeadLettersAsync(
        DeadLetterPurgeRequest request,
        CancellationToken cancellationToken = default,
        IProgress<DeadLetterPurgeProgress>? progress = null) =>
        WithWorkspaceAsync(workspace => workspace.PurgeDeadLettersAsync(request, cancellationToken, progress), cancellationToken);

    public Task<DeleteDeadLetterMessagesResult> DeleteDeadLetterMessagesAsync(
        DeleteDeadLetterMessagesRequest request,
        CancellationToken cancellationToken = default,
        IProgress<DeadLetterMessageDeletionProgress>? progress = null) =>
        WithWorkspaceAsync(workspace => workspace.DeleteDeadLetterMessagesAsync(request, cancellationToken, progress), cancellationToken);

    public QueueManagementCapabilities? QueueManagement => _current?.QueueManagement;

    public Task<QueueSettings> GetQueueSettingsAsync(string queue, CancellationToken cancellationToken = default) =>
        WithWorkspaceAsync(workspace => workspace.GetQueueSettingsAsync(queue, cancellationToken), cancellationToken);

    public Task CreateQueueAsync(QueueDefinition definition, CancellationToken cancellationToken = default) =>
        WithWorkspaceAsync(workspace => workspace.CreateQueueAsync(definition, cancellationToken), cancellationToken);

    public Task UpdateQueueSettingsAsync(string queue, QueueSettings settings, CancellationToken cancellationToken = default) =>
        WithWorkspaceAsync(workspace => workspace.UpdateQueueSettingsAsync(queue, settings, cancellationToken), cancellationToken);

    public Task DeleteQueueAsync(string queue, CancellationToken cancellationToken = default) =>
        WithWorkspaceAsync(workspace => workspace.DeleteQueueAsync(queue, cancellationToken), cancellationToken);

    public bool SupportsSubscriptionRules => _current?.SupportsSubscriptionRules == true;

    public RoutingService RoutingService => _current?.RoutingService ?? RoutingService.ServiceBus;

    public string? RuleEditingNote => _current?.RuleEditingNote;

    public Task<IReadOnlyList<SubscriptionRules>> GetTopicRulesAsync(string topic, CancellationToken cancellationToken = default) =>
        WithWorkspaceAsync(workspace => workspace.GetTopicRulesAsync(topic, cancellationToken), cancellationToken);

    public Task SaveSubscriptionRuleAsync(string topic, string subscription, SubscriptionRule rule, bool replace,
        CancellationToken cancellationToken = default) =>
        WithWorkspaceAsync(workspace => workspace.SaveSubscriptionRuleAsync(topic, subscription, rule, replace, cancellationToken), cancellationToken);

    public Task DeleteSubscriptionRuleAsync(string topic, string subscription, string rule, CancellationToken cancellationToken = default) =>
        WithWorkspaceAsync(workspace => workspace.DeleteSubscriptionRuleAsync(topic, subscription, rule, cancellationToken), cancellationToken);

    public Task DeleteSubscriptionRuleAsync(string topic, string subscription, SubscriptionRule rule, CancellationToken cancellationToken = default) =>
        WithWorkspaceAsync(workspace => workspace.DeleteSubscriptionRuleAsync(topic, subscription, rule, cancellationToken), cancellationToken);

    public Task<RemovePendingMessagesResult> RemovePendingMessagesAsync(
        IReadOnlyList<BrowsedMessage> messages,
        CancellationToken cancellationToken = default) =>
        WithWorkspaceAsync(workspace => workspace.RemovePendingMessagesAsync(messages, cancellationToken), cancellationToken);

    public Task<DeadLetterSnapshot> GetDeadLetterSnapshotAsync(
        DeadLetterMonitorScope scope,
        CancellationToken cancellationToken = default) =>
        WithWorkspaceAsync(workspace => workspace.GetDeadLetterSnapshotAsync(scope, cancellationToken), cancellationToken);

    private async Task WithWorkspaceAsync(Func<IServiceBusWorkspace, Task> action, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        await action(Current).ConfigureAwait(false);
    }

    private async Task<T> WithWorkspaceAsync<T>(Func<IServiceBusWorkspace, Task<T>> action, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        using var operation = await _operationGate.EnterOperationAsync(cancellationToken).ConfigureAwait(false);
        return await action(Current).ConfigureAwait(false);
    }

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) == 0)
        {
            _disposed = true;
            _ = CompleteDisposalAsync();
        }
        return new ValueTask(_disposalCompletion.Task);
    }

    private async Task CompleteDisposalAsync()
    {
        try
        {
            using var lifecycle = await _operationGate.EnterLifecycleAsync().ConfigureAwait(false);
            var errors = new List<Exception>();
            foreach (var workspace in _workspaces.Values)
            {
                try { await workspace.DisposeAsync().ConfigureAwait(false); }
                catch (Exception error) { errors.Add(error); }
            }
            _workspaces.Clear();
            _current = null;
            ConnectedProvider = null;
            if (errors.Count > 0) throw new AggregateException(errors);
            _disposalCompletion.TrySetResult();
        }
        catch (Exception error) { _disposalCompletion.TrySetException(error); }
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
