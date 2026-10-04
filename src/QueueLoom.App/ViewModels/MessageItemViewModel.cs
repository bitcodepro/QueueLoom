using System.Globalization;
using QueueLoom.App.Models;
using System.Text;
using System.Text.Json;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.App.ViewModels;

public sealed class MessageItemViewModel : ObservableObject
{
    private const int MaxEditablePayloadBytes = 1024 * 1024;
    private const int PreviewBytes = 4096;
    private const int MaxDisplayedProperties = 256;
    private const int MaxDisplayedPropertyCharacters = 4096;
    private static readonly JsonSerializerOptions IndentedJson = new()
    {
        WriteIndented = true,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    private readonly Lazy<EditableMessageBody> _displayBody;
    private readonly Lazy<string> _bodyPreview;
    private readonly Lazy<string> _bodyDisplay;
    private readonly Lazy<string> _applicationPropertiesJson;
    private readonly Lazy<string> _propertiesJson;
    private Lazy<DecodedBody?> _decoded;
    private bool _isMarked;

    public MessageItemViewModel(
        BrowsedMessage message,
        Guid? profileId = null,
        string? profileName = null,
        string? environmentLabel = null,
        Tone environmentTone = Tone.Neutral)
    {
        Message = message;
        ProfileId = profileId;
        ProfileName = profileName ?? string.Empty;
        EnvironmentLabel = environmentLabel ?? string.Empty;
        EnvironmentTone = environmentTone;
        _displayBody = new Lazy<EditableMessageBody>(
            () => EditableMessageBody.FromBytes(Message.Body.Span));
        _bodyPreview = new Lazy<string>(CreateBodyPreview);
        _bodyDisplay = new Lazy<string>(CreateBodyDisplay);
        _applicationPropertiesJson = new Lazy<string>(CreateApplicationPropertiesJson);
        _propertiesJson = new Lazy<string>(CreatePropertiesJson);
        _decoded = CreateDecoded();
    }

    private Lazy<DecodedBody?> CreateDecoded() => new(() => BodyDecoder.Decode(Message.Body, Message.Properties.ContentType, Message.Schema,
        messageType: ProtoSchemaCatalog.HintFrom(Message.Properties.ContentType, Message.ApplicationProperties)));

    /// <summary>Decodes the body again, for example after .proto files were loaded.</summary>
    public void RefreshDecoded()
    {
        if (!_decoded.IsValueCreated)
        {
            return;
        }
        _decoded = CreateDecoded();
        OnPropertyChanged(nameof(HasDecodedBody));
        OnPropertyChanged(nameof(DecodedText));
        OnPropertyChanged(nameof(DecodedIsJson));
        OnPropertyChanged(nameof(DecodedSteps));
        OnPropertyChanged(nameof(DecodedNote));
        OnPropertyChanged(nameof(HasDecodedNote));
        OnPropertyChanged(nameof(ShowsProtobufFieldNumbers));
    }

    public BrowsedMessage Message { get; }

    private (string Reason, string? Pattern)? _causeKey;

    /// <summary>The dead-letter reason and the pattern of its description, worked out once per message.</summary>
    public (string Reason, string? Pattern) CauseKey => _causeKey ??= DeadLetterCauses.KeyOf(Message);

    /// <summary>
    /// Ticked by the operator. Any message can be ticked to compare or export it; deleting, removing and resending
    /// still require every ticked message to be one those actions apply to (see <see cref="CanDelete"/>).
    /// </summary>
    public bool IsMarked
    {
        get => _isMarked;
        set => SetProperty(ref _isMarked, value);
    }

    /// <summary>
    /// Dead-lettered messages can be deleted, and scheduled or deferred ones cancelled or removed; other active
    /// messages are browse-only.
    /// </summary>
    public bool CanDelete => Message.IsDeadLetter || IsPending;

    /// <summary>A scheduled or deferred Azure Service Bus message: in the queue but not delivered to receivers.</summary>
    public bool IsPending => PendingMessages.IsPending(Message);

    public bool IsScheduled => IsPending && Message.State == ServiceBusMessageState.Scheduled;

    public bool IsDeferred => IsPending && Message.State == ServiceBusMessageState.Deferred;

    public bool IsDeadLetter => Message.IsDeadLetter;

    public DeadLetterMessageKey Key => new(Message.Source, Message.SubQueue, Message.SequenceNumber, Message.Properties.MessageId);

    public Guid? ProfileId { get; }

    public string ProfileName { get; }

    public string EnvironmentLabel { get; }

    public Tone EnvironmentTone { get; }

    public string SourceDisplay => Message.Source.DisplayName;

    public string SubQueueLabel => Message.SubQueue switch
    {
        ServiceBusSubQueue.TransferDeadLetter => "TRANSFER DLQ",
        ServiceBusSubQueue.DeadLetter => "DLQ",
        _ when IsScheduled => "SCHEDULED",
        _ when IsDeferred => "DEFERRED",
        _ => "ACTIVE"
    };

    /// <summary>The reason column: why a message was dead-lettered, or when a scheduled one will be delivered.</summary>
    public string StatusDetail => Message.IsDeadLetter
        ? DeadLetterReason
        : IsScheduled
            ? Message.Properties.ScheduledEnqueueTime is { } due
                ? $"Due {due.ToLocalTime().ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture)}"
                : "Scheduled"
            : IsDeferred
                ? "Waiting for its receiver"
                : "—";

    public long SequenceNumber => Message.SequenceNumber;

    /// <summary>SQS and Pub/Sub have no sequence numbers; their messages are told apart by message ID.</summary>
    public string SequenceDisplay => Message.HasSequenceNumber
        ? Message.SequenceNumber.ToString(System.Globalization.CultureInfo.CurrentCulture)
        : "—";

    /// <summary>The message ID; for Kafka records without one, where they are in the topic.</summary>
    public string MessageId => Message.Properties.MessageId ?? KafkaPosition ?? "(no MessageId)";

    private string? KafkaPosition =>
        Message.Position is { } position
            ? $"partition {position.Partition} · offset {position.Offset}"
            : null;

    public string Subject => Message.Properties.Subject ?? "—";

    public string EnqueuedAt => Message.EnqueuedAt?.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) ?? "—";

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

    /// <summary>
    /// The body as shown in the inspector: complete JSON bodies are indented for reading.
    /// Copy actions keep using <see cref="BodyText"/> so the original bytes are preserved.
    /// </summary>
    public string BodyDisplayText => _bodyDisplay.Value;

    /// <summary>The body unpacked (gzip, base64, Avro, Protobuf), when it is not readable as it is.</summary>
    public bool HasDecodedBody => _decoded.Value is not null;

    public string DecodedText => _decoded.Value?.Text ?? string.Empty;

    public bool DecodedIsJson => _decoded.Value?.IsJson == true;

    public IReadOnlyList<string> DecodedSteps => _decoded.Value?.Steps ?? [];

    public string DecodedNote => (_decoded.Value?.Note ?? string.Empty) +
                                 (Message.IsBodyTruncated && HasDecodedBody ? " Only the retained part of the body was decoded." : string.Empty);

    public bool HasDecodedNote => !string.IsNullOrWhiteSpace(DecodedNote);

    /// <summary>A Protobuf body shown by field numbers: .proto files would give the fields their names.</summary>
    public bool ShowsProtobufFieldNumbers => _decoded.Value is { } decoded &&
                                             (decoded.Steps.Contains("Protobuf (no schema)") ||
                                              decoded.Note?.Contains("shown by number", StringComparison.Ordinal) == true);

    public string BodyFormat => Message.IsBodyTruncated
        ? "Truncated preview"
        : _displayBody.Value.Format.ToString();

    public string BodyPreview => _bodyPreview.Value;

    public string ApplicationPropertiesJson => _applicationPropertiesJson.Value;

    public string PropertiesJson => _propertiesJson.Value;

    private string CreateBodyDisplay()
    {
        const int MaxFormattedBytes = 256 * 1024;
        if (Message.IsBodyTruncated ||
            Message.Body.Length > MaxFormattedBytes ||
            _displayBody.Value.Format != MessageBodyFormat.Json)
        {
            return BodyText;
        }

        try
        {
            using var document = JsonDocument.Parse(Message.Body);
            return JsonSerializer.Serialize(document.RootElement, IndentedJson);
        }
        catch (JsonException)
        {
            return BodyText;
        }
    }

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

    private string CreatePropertiesJson()
    {
        var broker = Message.Properties;
        return JsonSerializer.Serialize(
            new
            {
                source = new
                {
                    path = Message.Source.Path,
                    kind = Message.Source.Kind.ToString(),
                    subQueue = Message.SubQueue.ToString()
                },
                runtime = new
                {
                    position = Message.Position,
                    sequenceNumber = Message.SequenceNumber,
                    enqueuedSequenceNumber = Message.EnqueuedSequenceNumber,
                    state = Message.State.ToString(),
                    deliveryCount = Message.DeliveryCount,
                    enqueuedAt = Message.EnqueuedAt,
                    expiresAt = Message.ExpiresAt,
                    bodySize = Message.BodySize,
                    deadLetterReason = Message.DeadLetterReason,
                    deadLetterErrorDescription = Message.DeadLetterErrorDescription
                },
                broker = new
                {
                    broker.MessageId,
                    broker.CorrelationId,
                    broker.ContentType,
                    broker.Subject,
                    broker.To,
                    broker.ReplyTo,
                    broker.SessionId,
                    broker.ReplyToSessionId,
                    broker.PartitionKey,
                    broker.TransactionPartitionKey,
                    broker.AmqpType,
                    broker.AmqpAppId,
                    broker.TimeToLive,
                    broker.ScheduledEnqueueTime
                },
                applicationProperties = Message.ApplicationProperties
            },
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
