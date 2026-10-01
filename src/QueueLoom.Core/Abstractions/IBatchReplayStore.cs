using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Abstractions;

/// <summary>Durable, resumable copy operation. Implementations never settle original messages.</summary>
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

    Task<ReplayProgress> RunAsync(
        ReplayPlan plan,
        IServiceBusWorkspace workspace,
        Func<bool> canWrite,
        IProgress<ReplayProgress>? progress,
        CancellationToken token);
}
