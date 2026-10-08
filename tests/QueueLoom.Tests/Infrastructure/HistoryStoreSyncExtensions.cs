using QueueLoom.Core.Monitoring;

namespace QueueLoom.Tests.Infrastructure;

/// <summary>
/// Blocking forms of the history store for tests that check what the file holds, not how it is waited for. Tests run
/// without a UI synchronization context, so waiting here cannot deadlock; tests of contention use the async methods.
/// </summary>
internal static class HistoryStoreSyncExtensions
{
    public static void Append(this IDeadLetterHistoryStore store, DeadLetterHistorySample sample) =>
        store.AppendAsync(sample).GetAwaiter().GetResult();

    public static IReadOnlyList<DeadLetterHistorySample> Read(this IDeadLetterHistoryStore store, Guid profileId, DateTimeOffset since) =>
        store.ReadAsync(profileId, since).GetAwaiter().GetResult();
}
