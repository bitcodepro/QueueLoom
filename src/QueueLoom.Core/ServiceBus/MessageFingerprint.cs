using System.Security.Cryptography;
using System.Text;

namespace QueueLoom.Core.ServiceBus;

/// <summary>
/// A short hash of what a message contains: its body, standard properties and application properties (with their
/// types). Where a service has no real sequence numbers (RabbitMQ, SQS, Pub/Sub derive them from the Message ID), it
/// tells apart different messages that share a Message ID, and shows whether a message read again is the one chosen.
/// </summary>
public static class MessageFingerprint
{
    /// <summary>
    /// The fingerprint to hand out: the hash of everything the message carries, followed by the hash without the
    /// headers its reader established as broker-owned (<see cref="BrowsedMessage.BrokerOwnedHeaders"/>). A producer may
    /// set a header of the same name itself (RabbitMQ 4.2 classic queues keep an incoming x-acquired-count), so names
    /// alone prove nothing; the second part only recognizes the same message after the broker changed such a header
    /// (see <see cref="Find"/>).
    /// </summary>
    public static string Of(BrowsedMessage message) => Full(message) + Stable(message);

    /// <summary>The hash of everything the message carries.</summary>
    public static string Full(BrowsedMessage message) => Hash(message, skipVolatile: false);

    /// <summary>The hash without the headers the broker owns where the message was read.</summary>
    public static string Stable(BrowsedMessage message) => Hash(message, skipVolatile: true);

    /// <summary>
    /// The messages that are the one <paramref name="fingerprint"/> was handed out for: those identical to it, or
    /// otherwise the single message that differs from it only in headers its reader established as broker-owned (where
    /// nothing is, the second part equals the first and nothing else matches). Several such messages would be a guess,
    /// so <paramref name="ambiguous"/> is then set and nothing is returned.
    /// </summary>
    public static IReadOnlyList<BrowsedMessage> Find(string fingerprint, IEnumerable<BrowsedMessage> candidates, out bool ambiguous)
    {
        ArgumentNullException.ThrowIfNull(fingerprint);
        ArgumentNullException.ThrowIfNull(candidates);
        ambiguous = false;
        var all = candidates.ToArray();
        var full = fingerprint.Length == 32 ? fingerprint[..16] : fingerprint;
        var exact = all.Where(message => Full(message) == full).ToArray();
        if (exact.Length > 0 || fingerprint.Length != 32)
        {
            return exact;
        }
        var stable = all.Where(message => Stable(message) == fingerprint[16..]).ToArray();
        ambiguous = stable.Length > 1;
        return stable.Length == 1 ? stable : [];
    }

    private static string Hash(BrowsedMessage message, bool skipVolatile)
    {
        ArgumentNullException.ThrowIfNull(message);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        void Add(string? text)
        {
            var bytes = Encoding.UTF8.GetBytes(text ?? "\u0000");
            hash.AppendData(BitConverter.GetBytes(bytes.Length));
            hash.AppendData(bytes);
        }
        hash.AppendData(BitConverter.GetBytes(message.Body.Length));
        hash.AppendData(message.Body.Span);
        var properties = message.Properties;
        foreach (var value in new[]
                 {
                     properties.MessageId, properties.CorrelationId, properties.ContentType, properties.Subject, properties.To,
                     properties.ReplyTo, properties.SessionId, properties.ReplyToSessionId, properties.PartitionKey,
                     properties.TransactionPartitionKey, properties.AmqpType, properties.AmqpAppId, properties.NativeSubject,
                     properties.TimeToLive?.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture),
                     properties.ScheduledEnqueueTime?.UtcTicks.ToString(System.Globalization.CultureInfo.InvariantCulture)
                 })
        {
            Add(value);
        }
        foreach (var property in message.ApplicationProperties.Where(property => !skipVolatile || !message.BrokerOwnedHeaders.Contains(property.Name)))
        {
            Add(property.Name);
            Add(property.Type.ToString());
            Add(property.WireType);
            Add(property.Value);
        }
        // Added only when present, so the fingerprint of every message without them is what it was before they were read.
        if (properties.AmqpContentEncoding is { } encoding)
        {
            Add("amqp-content-encoding");
            Add(encoding);
        }
        if (properties.AmqpPriority is { } priority)
        {
            Add("amqp-priority");
            Add(priority.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
        return Convert.ToHexString(hash.GetHashAndReset())[..16].ToLowerInvariant();
    }
}
