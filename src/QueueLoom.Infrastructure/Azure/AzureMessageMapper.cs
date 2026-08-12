using System.Globalization;
using Azure.Messaging.ServiceBus;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Core.Validation;
using AzureMessageState = global::Azure.Messaging.ServiceBus.ServiceBusMessageState;
using DomainMessageState = QueueLoom.Core.ServiceBus.ServiceBusMessageState;

namespace QueueLoom.Infrastructure.Azure;

internal static class AzureMessageMapper
{
    internal const int MaxRetainedBodyBytes = 1024 * 1024;

    public static BrowsedMessage FromAzure(
        ServiceBusReceivedMessage message,
        ServiceBusEntityReference source,
        ServiceBusSubQueue subQueue)
    {
        var properties = new EditableMessageProperties(
            message.MessageId,
            message.CorrelationId,
            message.ContentType,
            message.Subject,
            message.To,
            message.ReplyTo,
            message.SessionId,
            message.ReplyToSessionId,
            message.PartitionKey,
            message.TransactionPartitionKey,
            message.TimeToLive == TimeSpan.MaxValue ? null : message.TimeToLive,
            message.ScheduledEnqueueTime == default ? null : message.ScheduledEnqueueTime);

        var body = message.Body.ToMemory();
        var retainedBody = body.Length > MaxRetainedBodyBytes
            ? body[..MaxRetainedBodyBytes]
            : body;

        return new BrowsedMessage(
            source,
            subQueue,
            message.SequenceNumber,
            retainedBody,
            properties,
            message.ApplicationProperties.Select(ToDomainProperty),
            MapState(message.State),
            message.EnqueuedSequenceNumber,
            message.DeliveryCount,
            message.EnqueuedTime == default ? null : message.EnqueuedTime,
            message.ExpiresAt == default ? null : message.ExpiresAt,
            message.LockedUntil == default ? null : message.LockedUntil,
            message.DeadLetterReason,
            message.DeadLetterErrorDescription,
            body.Length);
    }

    public static ServiceBusMessage ToAzure(MessageDraft draft)
    {
        ArgumentNullException.ThrowIfNull(draft);
        var validation = MessageDraftValidator.Validate(draft);
        if (!validation.IsValid)
        {
            throw new ArgumentException(
                string.Join(" ", validation.Errors.Select(error => error.Message)),
                nameof(draft));
        }

        var message = new ServiceBusMessage(new BinaryData(draft.Body.GetBytes()));
        var properties = draft.Properties;

        SetIfPresent(properties.MessageId, value => message.MessageId = value);
        SetIfPresent(properties.CorrelationId, value => message.CorrelationId = value);
        SetIfPresent(properties.ContentType, value => message.ContentType = value);
        SetIfPresent(properties.Subject, value => message.Subject = value);
        SetIfPresent(properties.To, value => message.To = value);
        SetIfPresent(properties.ReplyTo, value => message.ReplyTo = value);
        SetIfPresent(properties.SessionId, value => message.SessionId = value);
        SetIfPresent(properties.ReplyToSessionId, value => message.ReplyToSessionId = value);
        SetIfPresent(properties.PartitionKey, value => message.PartitionKey = value);
        SetIfPresent(properties.TransactionPartitionKey, value => message.TransactionPartitionKey = value);

        if (properties.TimeToLive is { } timeToLive)
        {
            message.TimeToLive = timeToLive;
        }
        if (properties.ScheduledEnqueueTime is { } scheduledAt)
        {
            message.ScheduledEnqueueTime = scheduledAt;
        }

        foreach (var property in draft.ApplicationProperties)
        {
            message.ApplicationProperties.Add(property.Name, ParseApplicationProperty(property));
        }

        return message;
    }

    internal static MessageApplicationProperty ToDomainProperty(KeyValuePair<string, object> property)
    {
        var (type, value) = property.Value switch
        {
            string typed => (ApplicationPropertyType.String, typed),
            bool typed => (ApplicationPropertyType.Boolean, typed.ToString(CultureInfo.InvariantCulture)),
            byte typed => (ApplicationPropertyType.Byte, typed.ToString(CultureInfo.InvariantCulture)),
            sbyte typed => (ApplicationPropertyType.SByte, typed.ToString(CultureInfo.InvariantCulture)),
            short typed => (ApplicationPropertyType.Int16, typed.ToString(CultureInfo.InvariantCulture)),
            ushort typed => (ApplicationPropertyType.UInt16, typed.ToString(CultureInfo.InvariantCulture)),
            int typed => (ApplicationPropertyType.Int32, typed.ToString(CultureInfo.InvariantCulture)),
            uint typed => (ApplicationPropertyType.UInt32, typed.ToString(CultureInfo.InvariantCulture)),
            long typed => (ApplicationPropertyType.Int64, typed.ToString(CultureInfo.InvariantCulture)),
            ulong typed => (ApplicationPropertyType.UInt64, typed.ToString(CultureInfo.InvariantCulture)),
            float typed => (ApplicationPropertyType.Single, typed.ToString("R", CultureInfo.InvariantCulture)),
            double typed => (ApplicationPropertyType.Double, typed.ToString("R", CultureInfo.InvariantCulture)),
            decimal typed => (ApplicationPropertyType.Decimal, typed.ToString(CultureInfo.InvariantCulture)),
            char typed => (ApplicationPropertyType.Character, typed.ToString()),
            Guid typed => (ApplicationPropertyType.Guid, typed.ToString("D")),
            DateTime typed => (ApplicationPropertyType.DateTime, typed.ToString("O", CultureInfo.InvariantCulture)),
            DateTimeOffset typed => (ApplicationPropertyType.DateTimeOffset, typed.ToString("O", CultureInfo.InvariantCulture)),
            TimeSpan typed => (ApplicationPropertyType.TimeSpan, typed.ToString("c", CultureInfo.InvariantCulture)),
            Uri typed => (ApplicationPropertyType.Uri, typed.ToString()),
            byte[] typed => (ApplicationPropertyType.Binary, Convert.ToBase64String(typed)),
            BinaryData typed => (ApplicationPropertyType.Binary, Convert.ToBase64String(typed.ToArray())),
            _ => (ApplicationPropertyType.String, Convert.ToString(property.Value, CultureInfo.InvariantCulture) ?? string.Empty)
        };

        return new MessageApplicationProperty(property.Key, type, value);
    }

    private static object ParseApplicationProperty(MessageApplicationProperty property) => property.Type switch
    {
        ApplicationPropertyType.String => property.Value,
        ApplicationPropertyType.Boolean => bool.Parse(property.Value),
        ApplicationPropertyType.Byte => byte.Parse(property.Value, CultureInfo.InvariantCulture),
        ApplicationPropertyType.SByte => sbyte.Parse(property.Value, CultureInfo.InvariantCulture),
        ApplicationPropertyType.Int16 => short.Parse(property.Value, CultureInfo.InvariantCulture),
        ApplicationPropertyType.UInt16 => ushort.Parse(property.Value, CultureInfo.InvariantCulture),
        ApplicationPropertyType.Int32 => int.Parse(property.Value, CultureInfo.InvariantCulture),
        ApplicationPropertyType.UInt32 => uint.Parse(property.Value, CultureInfo.InvariantCulture),
        ApplicationPropertyType.Int64 => long.Parse(property.Value, CultureInfo.InvariantCulture),
        ApplicationPropertyType.UInt64 => ulong.Parse(property.Value, CultureInfo.InvariantCulture),
        ApplicationPropertyType.Single => float.Parse(property.Value, CultureInfo.InvariantCulture),
        ApplicationPropertyType.Double => double.Parse(property.Value, CultureInfo.InvariantCulture),
        ApplicationPropertyType.Decimal => decimal.Parse(property.Value, CultureInfo.InvariantCulture),
        ApplicationPropertyType.Character => property.Value[0],
        ApplicationPropertyType.Guid => Guid.Parse(property.Value),
        ApplicationPropertyType.DateTime => DateTime.Parse(property.Value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
        ApplicationPropertyType.DateTimeOffset => DateTimeOffset.Parse(property.Value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
        ApplicationPropertyType.TimeSpan => TimeSpan.Parse(property.Value, CultureInfo.InvariantCulture),
        ApplicationPropertyType.Uri => new Uri(property.Value, UriKind.RelativeOrAbsolute),
        ApplicationPropertyType.Binary => Convert.FromBase64String(property.Value),
        _ => throw new ArgumentOutOfRangeException(nameof(property), property.Type, "Unsupported application property type.")
    };

    private static DomainMessageState MapState(AzureMessageState state) =>
        state switch
        {
            AzureMessageState.Active => DomainMessageState.Active,
            AzureMessageState.Deferred => DomainMessageState.Deferred,
            AzureMessageState.Scheduled => DomainMessageState.Scheduled,
            _ => DomainMessageState.Unknown
        };

    private static void SetIfPresent(string? value, Action<string> setter)
    {
        if (!string.IsNullOrWhiteSpace(value))
        {
            setter(value);
        }
    }
}
