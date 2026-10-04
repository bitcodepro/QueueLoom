using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Messaging;
using RabbitMQ.Client;

namespace QueueLoom.Infrastructure.RabbitMq;

internal static class RabbitMqMessageMapper
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    /// <summary>Legacy backup projection names. Basic metadata now travels separately from user headers.</summary>
    internal const string TypeProperty = "amqp-type";
    internal const string AppIdProperty = "amqp-app-id";

    /// <summary>Headers RabbitMQ writes when it dead-letters a message. They are shown, but not copied when resending.</summary>
    private static readonly string[] BrokerHeaderPrefixes = ["x-death", "x-first-death-", "x-last-death-", "x-delivery-count"];

    public static BrowsedMessage FromAmqp(
        ReadOnlyMemory<byte> body,
        IReadOnlyBasicProperties properties,
        string routingKey,
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue)
    {
        var headers = properties.Headers ?? new Dictionary<string, object?>();
        var death = FirstDeath(headers);
        var applicationProperties = headers
            .OrderBy(header => header.Key, StringComparer.Ordinal)
            .Select(header => ToProperty(header.Key, header.Value))
            .ToList();

        // A non-numeric expiration is already ignored. One that does not fit in a TimeSpan is the same: missing, not a failed read.
        TimeSpan? timeToLive = long.TryParse(properties.Expiration, NumberStyles.Integer, CultureInfo.InvariantCulture, out var milliseconds)
            ? BrokerClock.FromMilliseconds(milliseconds)
            : null;
        var messageProperties = new EditableMessageProperties(
            MessageId: properties.IsMessageIdPresent() ? properties.MessageId : null,
            CorrelationId: properties.IsCorrelationIdPresent() ? properties.CorrelationId : null,
            ContentType: properties.IsContentTypePresent() ? properties.ContentType : null,
            // The routing key the message was published with; for a dead letter, the one before it was dead-lettered.
            Subject: death?.RoutingKey ?? (routingKey.Length > 0 ? routingKey : null),
            ReplyTo: properties.IsReplyToPresent() ? properties.ReplyTo : null,
            TimeToLive: timeToLive,
            AmqpType: properties.IsTypePresent() ? properties.Type : null,
            AmqpAppId: properties.IsAppIdPresent() ? properties.AppId : null);

        var deliveryCount = headers.TryGetValue("x-delivery-count", out var count) && ToLong(count) is { } deliveries
            ? (int)Math.Min(deliveries, int.MaxValue)
            : (int)Math.Min(death?.Count ?? 0, int.MaxValue);
        return new BrowsedMessage(
            source,
            subQueue,
            LeasedMessageIdentity.SequenceNumberFor(messageProperties.MessageId ?? ContentIdentity(body, properties)),
            body,
            messageProperties,
            applicationProperties,
            ServiceBusMessageState.Active,
            deliveryCount: deliveryCount,
            // Publishers sometimes store milliseconds in the seconds field. That is still a message; the time is omitted.
            enqueuedAt: properties.IsTimestampPresent() ? BrokerClock.FromUnixSeconds(properties.Timestamp.UnixTime) : null,
            deadLetterReason: death?.Reason,
            deadLetterErrorDescription: death is null
                ? null
                : $"From {death.Queue ?? "(unknown queue)"}" + (death.Count > 1 ? $", {death.Count} times" : string.Empty) +
                  (death.Time is { } time ? $", at {time.ToLocalTime():yyyy-MM-dd HH:mm:ss}" : string.Empty))
        { HasSequenceNumber = false };
    }

    /// <summary>The queue the message was dead-lettered from (the newest x-death entry), if any.</summary>
    public static string? DeadLetteredFrom(IReadOnlyBasicProperties properties) =>
        properties.Headers is { } headers ? FirstDeath(headers)?.Queue : null;

    public static BasicProperties ToAmqp(MessageDraft message)
    {
        var draft = message.Properties;
        var properties = new BasicProperties
        {
            MessageId = draft.MessageId,
            CorrelationId = draft.CorrelationId,
            ContentType = draft.ContentType,
            ReplyTo = draft.ReplyTo,
            Type = draft.AmqpType,
            AppId = draft.AmqpAppId,
            Persistent = true,
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            Headers = new Dictionary<string, object?>(StringComparer.Ordinal)
        };
        if (draft.TimeToLive is { } timeToLive)
        {
            properties.Expiration = ((long)timeToLive.TotalMilliseconds).ToString(CultureInfo.InvariantCulture);
        }

        foreach (var property in message.ApplicationProperties)
        {
            // Historical persisted drafts have no separate envelope. Keep their prior interpretation,
            // including the ambiguity of a single user header with one of these names.
            if (message.LegacyAmqpMetadata && property.Name == TypeProperty) { properties.Type = property.Value; continue; }
            if (message.LegacyAmqpMetadata && property.Name == AppIdProperty) { properties.AppId = property.Value; continue; }
            if (BrokerHeaderPrefixes.Any(prefix => property.Name.StartsWith(prefix, StringComparison.Ordinal)))
            {
                continue;
            }
            properties.Headers[property.Name] = ToHeaderValue(property);
        }
        return properties;
    }

    private static object ToHeaderValue(MessageApplicationProperty property) => property.Type switch
    {
        ApplicationPropertyType.Boolean => bool.Parse(property.Value),
        ApplicationPropertyType.Byte or ApplicationPropertyType.SByte or ApplicationPropertyType.Int16 or ApplicationPropertyType.UInt16
            or ApplicationPropertyType.Int32 => int.Parse(property.Value, CultureInfo.InvariantCulture),
        ApplicationPropertyType.UInt32 or ApplicationPropertyType.Int64 => long.Parse(property.Value, CultureInfo.InvariantCulture),
        ApplicationPropertyType.Single or ApplicationPropertyType.Double => double.Parse(property.Value, CultureInfo.InvariantCulture),
        ApplicationPropertyType.Decimal => decimal.Parse(property.Value, ApplicationPropertyValues.DecimalStyle, CultureInfo.InvariantCulture),
        ApplicationPropertyType.DateTime or ApplicationPropertyType.DateTimeOffset =>
            new AmqpTimestamp(DateTimeOffset.Parse(property.Value, CultureInfo.InvariantCulture).ToUnixTimeSeconds()),
        ApplicationPropertyType.Binary => Convert.FromBase64String(property.Value),
        _ => Encoding.UTF8.GetBytes(property.Value)
    };

    private static MessageApplicationProperty ToProperty(string name, object? value) => value switch
    {
        byte[] bytes => ByteProperty(name, bytes),
        string text => new MessageApplicationProperty(name, ApplicationPropertyType.String, text),
        bool flag => new MessageApplicationProperty(name, ApplicationPropertyType.Boolean, flag ? "true" : "false"),
        int number => new MessageApplicationProperty(name, ApplicationPropertyType.Int32, number.ToString(CultureInfo.InvariantCulture)),
        long number => new MessageApplicationProperty(name, ApplicationPropertyType.Int64, number.ToString(CultureInfo.InvariantCulture)),
        double number => new MessageApplicationProperty(name, ApplicationPropertyType.Double, number.ToString("R", CultureInfo.InvariantCulture)),
        float number => new MessageApplicationProperty(name, ApplicationPropertyType.Single, number.ToString("R", CultureInfo.InvariantCulture)),
        decimal number => new MessageApplicationProperty(name, ApplicationPropertyType.Decimal, number.ToString(CultureInfo.InvariantCulture)),
        // A timestamp outside the calendar stays the raw seconds, so one header cannot fail the whole message.
        AmqpTimestamp timestamp => BrokerClock.FromUnixSeconds(timestamp.UnixTime) is { } at
            ? new MessageApplicationProperty(name, ApplicationPropertyType.DateTimeOffset, at.ToString("O", CultureInfo.InvariantCulture))
            : new MessageApplicationProperty(name, ApplicationPropertyType.Int64, timestamp.UnixTime.ToString(CultureInfo.InvariantCulture)),
        null => new MessageApplicationProperty(name, ApplicationPropertyType.String, string.Empty),
        _ => new MessageApplicationProperty(name, ApplicationPropertyType.String, JsonSerializer.Serialize(ToJsonValue(value)))
    };

    private static MessageApplicationProperty ByteProperty(string name, byte[] bytes)
    {
        try
        {
            return new MessageApplicationProperty(name, ApplicationPropertyType.String, StrictUtf8.GetString(bytes));
        }
        catch (DecoderFallbackException)
        {
            return new MessageApplicationProperty(name, ApplicationPropertyType.Binary, Convert.ToBase64String(bytes));
        }
    }

    private static object? ToJsonValue(object? value) => value switch
    {
        byte[] bytes => Encoding.UTF8.GetString(bytes),
        AmqpTimestamp timestamp => BrokerClock.FromUnixSeconds(timestamp.UnixTime) ?? (object)timestamp.UnixTime,
        IDictionary<string, object?> table => table.ToDictionary(pair => pair.Key, pair => ToJsonValue(pair.Value)),
        IEnumerable<object?> list => list.Select(ToJsonValue).ToArray(),
        _ => value
    };

    private sealed record Death(string? Queue, string Reason, long Count, DateTimeOffset? Time, string? RoutingKey);

    private static Death? FirstDeath(IDictionary<string, object?> headers)
    {
        if (!headers.TryGetValue("x-death", out var value) || value is not IEnumerable<object?> entries)
        {
            return null;
        }

        foreach (var item in entries)
        {
            // A malformed newest entry cannot establish ownership; never infer it from an older death.
            if (item is not IDictionary<string, object?> entry) return null;
            string? Text(string key)
            {
                if (!entry.TryGetValue(key, out var item)) return null;
                try
                {
                    return item switch { byte[] bytes => StrictUtf8.GetString(bytes), string text => text, _ => null };
                }
                catch (DecoderFallbackException) { return null; }
            }

            var routingKeys = entry.TryGetValue("routing-keys", out var keys) && keys is IEnumerable<object?> list
                ? list.Select(key => key is byte[] bytes ? Encoding.UTF8.GetString(bytes) : key?.ToString()).FirstOrDefault()
                : null;
            return new Death(
                Text("queue"),
                Text("reason") switch
                {
                    "rejected" => "Rejected by a consumer",
                    "expired" => "Expired (message TTL)",
                    "maxlen" => "Queue length limit reached",
                    "delivery_limit" => "Delivery limit reached",
                    var other => other ?? "Dead-lettered"
                },
                entry.TryGetValue("count", out var count) ? ToLong(count) ?? 1 : 1,
                entry.TryGetValue("time", out var time) && time is AmqpTimestamp stamp ? BrokerClock.FromUnixSeconds(stamp.UnixTime) : null,
                routingKeys);
        }
        return null;
    }

    private static long? ToLong(object? value) => value switch
    {
        long number => number,
        int number => number,
        short number => number,
        byte number => number,
        _ => null
    };

    /// <summary>A stable key for messages without a message ID: the body and the properties that do not change while it waits.</summary>
    private static string ContentIdentity(ReadOnlyMemory<byte> body, IReadOnlyBasicProperties properties)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        hash.AppendData(body.Span);
        hash.AppendData(Encoding.UTF8.GetBytes(
            $"|{properties.CorrelationId}|{properties.Type}|{(properties.IsTimestampPresent() ? properties.Timestamp.UnixTime : 0)}|" +
            string.Join(",", (properties.Headers ?? new Dictionary<string, object?>()).Keys.Order(StringComparer.Ordinal))));
        return "content:" + Convert.ToHexString(hash.GetHashAndReset());
    }
}
