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
    /// Headers the broker itself changes while a message waits: a RabbitMQ quorum queue counts every acquisition
    /// (reading it to show it is one), so they are not part of what the message is.
    /// </summary>
    public static readonly IReadOnlySet<string> VolatileHeaders = new HashSet<string>(StringComparer.Ordinal)
    {
        "x-delivery-count", "x-acquired-count"
    };

    public static string Of(BrowsedMessage message)
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
        foreach (var property in message.ApplicationProperties.Where(property => !VolatileHeaders.Contains(property.Name)))
        {
            Add(property.Name);
            Add(property.Type.ToString());
            Add(property.WireType);
            Add(property.Value);
        }
        return Convert.ToHexString(hash.GetHashAndReset())[..16].ToLowerInvariant();
    }
}
