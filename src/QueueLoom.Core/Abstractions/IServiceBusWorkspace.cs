using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Abstractions;

public interface IServiceBusWorkspace : IAsyncDisposable
{
    WorkspaceConnectionState ConnectionState { get; }

    Guid? ConnectedProfileId { get; }
    string? ConnectedNamespace => null;

    Task ConnectAsync(ServiceBusProfile profile, CancellationToken cancellationToken = default);

    Task DisconnectAsync(CancellationToken cancellationToken = default);

    Task SetAccessModeAsync(
        ProfileAccessMode accessMode,
        CancellationToken cancellationToken = default);

    Task<ServiceBusTopology> GetTopologyAsync(
        bool forceRefresh = false,
        CancellationToken cancellationToken = default);

    Task<IReadOnlyList<BrowsedMessage>> BrowseMessagesAsync(
        BrowseMessagesRequest request,
        CancellationToken cancellationToken = default);

    Task<DeadLetterSearchResult> SearchDeadLettersAsync(
        DeadLetterSearchRequest request,
        CancellationToken cancellationToken = default);

    Task SendMessageAsync(
        SendMessageRequest request,
        CancellationToken cancellationToken = default);

    Task ResubmitDeadLetterAsync(
        ResubmitDeadLetterRequest request,
        CancellationToken cancellationToken = default);

    Task<DeadLetterPurgeResult> PurgeDeadLettersAsync(
        DeadLetterPurgeRequest request,
        CancellationToken cancellationToken = default,
        IProgress<DeadLetterPurgeProgress>? progress = null);

    /// <summary>
    /// Backs up and deletes exactly the given dead-lettered messages, leaving every other message in place.
    /// </summary>
    Task<DeleteDeadLetterMessagesResult> DeleteDeadLetterMessagesAsync(
        DeleteDeadLetterMessagesRequest request,
        CancellationToken cancellationToken = default,
        IProgress<DeadLetterMessageDeletionProgress>? progress = null);

    /// <summary>
    /// Backs up, then cancels scheduled messages and removes deferred ones (Azure Service Bus only). Every message
    /// must satisfy <see cref="PendingMessages.IsPending"/>.
    /// </summary>
    Task<RemovePendingMessagesResult> RemovePendingMessagesAsync(
        IReadOnlyList<BrowsedMessage> messages,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("Only Azure Service Bus has scheduled and deferred messages.");

    /// <summary>What queue management this service offers; null when it offers none.</summary>
    QueueManagementCapabilities? QueueManagement => null;

    Task<QueueSettings> GetQueueSettingsAsync(string queue, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("This service does not support queue management in QueueLoom.");

    Task CreateQueueAsync(QueueDefinition definition, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("This service does not support queue management in QueueLoom.");

    Task UpdateQueueSettingsAsync(string queue, QueueSettings settings, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("This service does not support queue management in QueueLoom.");

    /// <summary>Deletes the queue and every message in it. Its dead-letter queue, if separate, is kept.</summary>
    Task DeleteQueueAsync(string queue, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("This service does not support queue management in QueueLoom.");

    /// <summary>
    /// True where topic subscriptions have rules that filter what they receive: Service Bus rules, SNS filter
    /// policies, Pub/Sub filters and RabbitMQ bindings.
    /// </summary>
    bool SupportsSubscriptionRules => false;

    /// <summary>Whose rules they are, which decides how they are read and checked.</summary>
    Routing.RoutingService RoutingService => Routing.RoutingService.ServiceBus;

    /// <summary>Why rules cannot be changed here even with queue management allowed, or null when they can.</summary>
    string? RuleEditingNote => null;

    /// <summary>Every subscription of the topic with its rules.</summary>
    Task<IReadOnlyList<Routing.SubscriptionRules>> GetTopicRulesAsync(string topic, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("This service has no subscription rules.");

    /// <summary>Adds a rule, or replaces the filter and action of the rule with the same name.</summary>
    Task SaveSubscriptionRuleAsync(string topic, string subscription, Routing.SubscriptionRule rule, bool replace,
        CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("This service has no subscription rules.");

    Task DeleteSubscriptionRuleAsync(string topic, string subscription, string rule, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("This service has no subscription rules.");

    /// <summary>Deletes the rule as it was read, so the service can tell which destination it belongs to.</summary>
    Task DeleteSubscriptionRuleAsync(string topic, string subscription, Routing.SubscriptionRule rule, CancellationToken cancellationToken = default) =>
        DeleteSubscriptionRuleAsync(topic, subscription, rule.Name, cancellationToken);

    Task<DeadLetterSnapshot> GetDeadLetterSnapshotAsync(
        DeadLetterMonitorScope scope,
        CancellationToken cancellationToken = default);
}
