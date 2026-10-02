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

    /// <summary>Receives and holds up to <paramref name="maxMessages"/> messages. An empty result means none arrived in a short wait.</summary>
    Task<IReadOnlyList<LeasedMessage>> ReceiveAsync(int maxMessages, CancellationToken cancellationToken);

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
