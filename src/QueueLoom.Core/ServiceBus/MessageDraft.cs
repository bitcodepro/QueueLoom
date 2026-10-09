namespace QueueLoom.Core.ServiceBus;

public sealed record MessageDraft
{
    public MessageDraft(
        EditableMessageBody body,
        EditableMessageProperties? properties = null,
        IEnumerable<MessageApplicationProperty>? applicationProperties = null)
    {
        ArgumentNullException.ThrowIfNull(body);

        Body = body;
        Properties = properties ?? EditableMessageProperties.Empty;
        ApplicationProperties = Array.AsReadOnly((applicationProperties ?? []).ToArray());
    }

    public EditableMessageBody Body { get; }

    public EditableMessageProperties Properties { get; }

    public IReadOnlyList<MessageApplicationProperty> ApplicationProperties { get; }
    public KafkaEnvelope? KafkaEnvelope { get; init; }
    /// <summary>Only older persisted drafts used application-property names for AMQP basic metadata.</summary>
    public bool LegacyAmqpMetadata { get; init; }
    /// <summary>Older persisted drafts used the historical exact x-delivery-count removal policy.</summary>
    public bool LegacyAmqpBrokerHeaders { get; init; }

    public static MessageDraft Empty { get; } = new(EditableMessageBody.Empty);
}
