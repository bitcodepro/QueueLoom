using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Abstractions;

/// <summary>Durable replay and resend snapshots. Only proven rejections and unattempted items can send.
/// Moves use the workspace's backed-up, identity-checked deletion after send acknowledgement.</summary>
public interface IBatchReplayStore
{
    string RootDirectory { get; }

    Task<ReplayPlan> CreateAsync(
        Guid profileId,
        ServiceBusEntityReference destination,
        IEnumerable<(MessageDraft Draft, string Origin)> drafts,
        bool preserveIds,
        int rate,
        CancellationToken token,
        string? fullyQualifiedNamespace = null,
        string? configurationIdentity = null);

    ReplayPlan? Latest(Guid profileId);

    IReadOnlyList<ReplayPlan> List();
    OperationHistory ReadHistory(ReplayPlan plan);
    Task<ReplayPlan> CreateResendAsync(Guid profileId, IReadOnlyList<ResendItem> items, ResendMode mode,
        int rate, string? ns, string configurationIdentity, string kind, CancellationToken token, bool deferActivation = false);
    Task ActivateScheduledAsync(ReplayPlan plan, CancellationToken token);
    Task<ResendResult> RunItemsAsync(ReplayPlan plan, IReadOnlyList<int> indexes, bool retryRejected,
        IServiceBusWorkspace workspace, Func<bool> canWrite, IProgress<ResendProgress>? progress, CancellationToken token);

    Task<ReplayProgress> RunAsync(
        ReplayPlan plan,
        IServiceBusWorkspace workspace,
        Func<bool> canWrite,
        IProgress<ReplayProgress>? progress,
        CancellationToken token);
}
