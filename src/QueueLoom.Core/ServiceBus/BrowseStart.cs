namespace QueueLoom.Core.ServiceBus;

public enum BrowseStartKind
{
    /// <summary>From the oldest message the service still keeps.</summary>
    Oldest,

    /// <summary>The most recent messages; the next page goes further back.</summary>
    Newest,

    /// <summary>From the first message written at or after <see cref="BrowseStart.Time"/>.</summary>
    FromTime,

    /// <summary>From <see cref="BrowseStart.Offset"/>, in one partition or in all of them.</summary>
    FromOffset
}

/// <summary>
/// Where reading starts in a log that keeps its messages (Kafka). Services that hand out messages in their own
/// order ignore it. <see cref="Positions"/> continues a previous page: per partition, the next offset to read when
/// going forward, or the offset to stop before when reading <see cref="BrowseStartKind.Newest"/>.
/// </summary>
public sealed record BrowseStart(
    BrowseStartKind Kind,
    DateTimeOffset? Time = null,
    long? Offset = null,
    int? Partition = null)
{
    public static BrowseStart Oldest { get; } = new(BrowseStartKind.Oldest);

    public IReadOnlyDictionary<int, long>? Positions { get; init; }

    public bool IsDefault => Kind == BrowseStartKind.Oldest && Positions is null;
}
