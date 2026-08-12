using System.Text;
using System.Text.Json;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

public sealed class MessageItemViewModel
{
    private const int MaxEditablePayloadBytes = 1024 * 1024;
    private const int PreviewBytes = 4096;
    private const int MaxDisplayedProperties = 256;
    private const int MaxDisplayedPropertyCharacters = 4096;

    private readonly Lazy<EditableMessageBody> _displayBody;
    private readonly Lazy<string> _bodyPreview;
    private readonly Lazy<string> _applicationPropertiesJson;

    public MessageItemViewModel(
        BrowsedMessage message,
        Guid? profileId = null,
        string? profileName = null,
        string? environmentLabel = null,
        string? environmentColor = null)
    {
        Message = message;
        ProfileId = profileId;
        ProfileName = profileName ?? string.Empty;
        EnvironmentLabel = environmentLabel ?? string.Empty;
        EnvironmentColor = environmentColor ?? "#91A5BD";
        _displayBody = new Lazy<EditableMessageBody>(
            () => EditableMessageBody.FromBytes(Message.Body.Span));
        _bodyPreview = new Lazy<string>(CreateBodyPreview);
        _applicationPropertiesJson = new Lazy<string>(CreateApplicationPropertiesJson);
    }

    public BrowsedMessage Message { get; }

    public Guid? ProfileId { get; }

    public string ProfileName { get; }

    public string EnvironmentLabel { get; }

    public string EnvironmentColor { get; }

    public string SourceDisplay => Message.Source.DisplayName;

    public string SubQueueLabel => Message.SubQueue == ServiceBusSubQueue.TransferDeadLetter
        ? "TRANSFER DLQ"
        : "DLQ";

    public long SequenceNumber => Message.SequenceNumber;

    public string MessageId => Message.Properties.MessageId ?? "(no MessageId)";

    public string Subject => Message.Properties.Subject ?? "—";

    public string EnqueuedAt => Message.EnqueuedAt?.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss") ?? "—";

    public string DeadLetterReason => Message.DeadLetterReason ?? "—";

    public string DeadLetterDescription => Message.DeadLetterErrorDescription ?? string.Empty;

    public string ContentType => Message.Properties.ContentType ?? "—";

    public string CorrelationId => Message.Properties.CorrelationId ?? "—";

    public int DeliveryCount => Message.DeliveryCount;

    public bool CanOpenAsDraft =>
        !Message.IsBodyTruncated && EstimateEditablePayloadBytes() <= MaxEditablePayloadBytes;

    public string EditLimitText => CanOpenAsDraft
        ? string.Empty
        : $"Read-only preview: payload is {Message.BodySize:N0} bytes; the safe editor limit is {MaxEditablePayloadBytes:N0} bytes.";

    public string BodyText => Message.IsBodyTruncated
        ? _displayBody.Value.Content +
          $"\n\n[QueueLoom preview truncated at {Message.Body.Length:N0} of {Message.BodySize:N0} bytes]"
        : _displayBody.Value.Content;

    public string BodyFormat => Message.IsBodyTruncated
        ? "Truncated preview"
        : _displayBody.Value.Format.ToString();

    public string BodyPreview => _bodyPreview.Value;

    public string ApplicationPropertiesJson => _applicationPropertiesJson.Value;

    private string CreateBodyPreview()
    {
        var length = Math.Min(Message.Body.Length, PreviewBytes);
        var display = EditableMessageBody.FromBytes(Message.Body.Span[..length]).Content;
        var preview = display.Replace('\r', ' ').Replace('\n', ' ').Trim();
        return preview.Length > 180 ? preview[..180] + "…" : preview;
    }

    private string CreateApplicationPropertiesJson()
    {
        var properties = Message.ApplicationProperties
            .Take(MaxDisplayedProperties)
            .ToDictionary(
                property => property.Name,
                property => new
                {
                    type = property.Type.ToString(),
                    value = property.Value.Length > MaxDisplayedPropertyCharacters
                        ? property.Value[..MaxDisplayedPropertyCharacters] + "… [display truncated]"
                        : property.Value
                },
                StringComparer.Ordinal);

        return JsonSerializer.Serialize(
            properties,
            new JsonSerializerOptions { WriteIndented = true });
    }

    private long EstimateEditablePayloadBytes()
    {
        var size = Message.BodySize;
        foreach (var property in Message.ApplicationProperties)
        {
            size = checked(size + Encoding.UTF8.GetByteCount(property.Name));
            size = checked(size + Encoding.UTF8.GetByteCount(property.Value));
        }
        return size;
    }
}
