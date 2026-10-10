namespace QueueLoom.Core.Monitoring;

/// <summary>
/// Where each partition of a dead-letter topic ends (Kafka): a checkpoint, not message identities. Records only arrive
/// at the end, so an end that moved forward means that many offsets were written since, whatever retention removed.
/// </summary>
public sealed record DeadLetterOffsets(string Topic, IReadOnlyDictionary<int, long> Ends)
{
    /// <summary>
    /// How many offsets were written between two checkpoints of the same topic, or null when they do not compare
    /// (another topic, or an end that moved back: the topic was recreated). A partition added since starts at 0. Offsets
    /// also count transaction markers, so this is at most the number of new messages.
    /// </summary>
    public static long? Arrivals(DeadLetterOffsets? before, DeadLetterOffsets? now)
    {
        if (before is null || now is null || !string.Equals(before.Topic, now.Topic, StringComparison.Ordinal))
        {
            return null;
        }
        long arrivals = 0;
        foreach (var (partition, end) in now.Ends)
        {
            var start = before.Ends.TryGetValue(partition, out var earlier) ? earlier : 0;
            if (end < start)
            {
                return null;
            }
            arrivals += end - start;
        }
        return arrivals;
    }
}
