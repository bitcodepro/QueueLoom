using System.Security.Cryptography;
using System.Text;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Infrastructure.Messaging;

/// <summary>
/// A message received from a service that has no non-destructive peek (Amazon SQS, Google Pub/Sub).
/// It stays invisible to other consumers until it is released back or settled (deleted/acknowledged).
/// </summary>
/// <param name="Message">The message as QueueLoom shows it.</param>
/// <param name="LeaseHandle">The receipt handle (SQS) or ack ID (Pub/Sub).</param>
/// <param name="BelongsToSource">
/// False when a dead-letter queue is shared by several sources and this message came from another one.
/// Such messages are released untouched.
/// </param>
public sealed record LeasedMessage(BrowsedMessage Message, string LeaseHandle, bool BelongsToSource = true)
{
    public string? DeliveryIdentity { get; init; }
    public string Identity => DeliveryIdentity ?? Message.Properties.MessageId ?? LeaseHandle;
}

/// <summary>One readable place: a queue, a subscription or the dead-letter destination behind one of them.</summary>
public interface ILeasedMessageChannel
{
    /// <summary>The physical queue or subscription name, used in messages to the operator.</summary>
    string PhysicalName { get; }

    int MaximumBatchSize { get; }
    /// <summary>Why the channel cannot safely attribute deliveries to the requested source.</summary>
    string? SourceAttributionError => null;

    /// <summary>
    /// True for an SQS FIFO queue: while messages of a group are held, SQS hands out no further messages of that group,
    /// so running dry does not prove that every message was seen.
    /// </summary>
    bool ReadsOneBatchPerMessageGroup => false;

    /// <summary>Receives and holds up to <paramref name="maxMessages"/> messages. An empty result means none arrived in a short wait.</summary>
    Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int maxMessages, CancellationToken cancellationToken);

    /// <summary>
    /// How long a received message stays held unless the hold is renewed; null when holds do not expire. Long scans
    /// renew what they still hold before this ends, or early messages would come back and be received again.
    /// </summary>
    TimeSpan? HoldDuration => null;

    /// <summary>
    /// Extends the hold of messages still held, best effort: a message whose renewal fails comes back when its current
    /// hold ends, as it would have without the renewal.
    /// </summary>
    Task RenewAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>Makes held messages visible again right away, without changing them.</summary>
    Task ReleaseAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken);

    /// <summary>Deletes (SQS) or acknowledges (Pub/Sub) held messages. Returns the ones that could not be settled.</summary>
    Task<IReadOnlyCollection<LeasedMessage>> SettleAsync(IReadOnlyCollection<LeasedMessage> messages, CancellationToken cancellationToken);
}

public static class LeasedMessageIdentity
{
    /// <summary>
    /// SQS and Pub/Sub identify messages by string IDs, while QueueLoom keys messages by a sequence number.
    /// A stable non-negative hash of the message ID fills that role; deletion always re-checks the message ID.
    /// </summary>
    public static long SequenceNumberFor(string messageId)
    {
        ArgumentException.ThrowIfNullOrEmpty(messageId);
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(messageId));
        return BitConverter.ToInt64(hash, 0) & long.MaxValue;
    }
}

/// <summary>
/// Renews the holds of a long-running loop before they end: every third of the channel's hold duration, everything still
/// held is renewed, so messages received early do not come back and get received (and, on Pub/Sub, counted) again.
/// The interval is measured from the start of the previous renewal (its first leases are the oldest), and a third leaves
/// room for the renewal itself to take time.
/// </summary>
internal sealed class HoldRenewal(ILeasedMessageChannel channel, TimeProvider time)
{
    private long _since = time.GetTimestamp();

    public async Task RenewIfDueAsync(IReadOnlyCollection<LeasedMessage> held, CancellationToken cancellationToken)
    {
        if (channel.HoldDuration is not { } hold || held.Count == 0 || time.GetElapsedTime(_since) < hold / 3)
        {
            return;
        }
        var started = time.GetTimestamp();
        await channel.RenewAsync([.. held], cancellationToken).ConfigureAwait(false);
        _since = started;
    }
}
