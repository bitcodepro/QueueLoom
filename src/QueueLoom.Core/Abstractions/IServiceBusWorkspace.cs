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

    Task<DeadLetterSnapshot> GetDeadLetterSnapshotAsync(
        DeadLetterMonitorScope scope,
        CancellationToken cancellationToken = default);
}
