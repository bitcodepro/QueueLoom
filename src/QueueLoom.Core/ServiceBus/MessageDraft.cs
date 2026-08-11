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

    public static MessageDraft Empty { get; } = new(EditableMessageBody.Empty);
}
