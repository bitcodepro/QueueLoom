using QueueLoom.Core.Monitoring;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Abstractions;

public interface IServiceBusWorkspace : IAsyncDisposable
{
    WorkspaceConnectionState ConnectionState { get; }

    Guid? ConnectedProfileId { get; }

    Task ConnectAsync(ServiceBusProfile profile, CancellationToken cancellationToken = default);

    Task DisconnectAsync(CancellationToken cancellationToken = default);

    Task<ServiceBusTopology> GetTopologyAsync(
        bool forceRefresh = false,
        CancellationToken cancellationToken = default);

    Task<IReadOnlyList<BrowsedMessage>> BrowseMessagesAsync(
        BrowseMessagesRequest request,
        CancellationToken cancellationToken = default);

    Task SendMessageAsync(
        SendMessageRequest request,
        CancellationToken cancellationToken = default);

    Task ResubmitDeadLetterAsync(
        ResubmitDeadLetterRequest request,
        CancellationToken cancellationToken = default);

    Task<DeadLetterSnapshot> GetDeadLetterSnapshotAsync(
        DeadLetterMonitorScope scope,
        CancellationToken cancellationToken = default);
}
