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
        var validation = MessageDraftValidator.Validate(draft, QueueLoom.Core.Profiles.MessagingProvider.AzureServiceBus);
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

    internal static MessageApplicationProperty ToDomainProperty(KeyValuePair<string, object> property) =>
        ApplicationPropertyValues.FromObject(property.Key, property.Value is BinaryData binary ? binary.ToArray() : property.Value);

    /// <summary>
    /// The value Service Bus is given. A Binary property (a Kafka or RabbitMQ header, for example) is sent as its
    /// Base64 text: the service rejects byte[] application properties with MessageSizeExceeded, and the SDK documents
    /// Base64 text as the workaround. The bytes are kept exactly, only their representation changes.
    /// </summary>
    private static object ParseApplicationProperty(MessageApplicationProperty property) =>
        property.Type == ApplicationPropertyType.Binary
            ? Convert.ToBase64String(Convert.FromBase64String(property.Value))
            : ApplicationPropertyValues.ToObject(property);

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
