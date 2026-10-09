using QueueLoom.Core;
using System.Globalization;
using System.Text;
using System.Text.Json;
using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Validation;

public static class MessageDraftValidator
{
    /// <summary>Azure Service Bus identifiers; also SQS/SNS FIFO MessageGroupId and MessageDeduplicationId.</summary>
    public const int MaxMessageIdentifierLength = 128;

    /// <summary>Google Pub/Sub: "An ordering key can be up to 1 KB in length."</summary>
    public const int MaxPubSubOrderingKeyBytes = 1024;

    /// <summary>AMQP 0-9-1 short string (RabbitMQ message-id, correlation-id, content-type, reply-to, type, app-id).</summary>
    public const int MaxAmqpShortStringBytes = 255;

    /// <summary>RabbitMQ's largest per-message expiration (MAX_EXPIRY_TIMER), in milliseconds.</summary>
    public const long MaxRabbitMqExpirationMilliseconds = 315_360_000_000;

    /// <summary>Google Pub/Sub quotas: 100 attributes per message, keys up to 256 bytes, values up to 1,024 bytes.</summary>
    public const int MaxPubSubAttributes = 100;
    public const int MaxPubSubAttributeKeyBytes = 256;
    public const int MaxPubSubAttributeValueBytes = 1024;

    /// <summary>SQS and SNS message attribute names: up to 256 characters.</summary>
    public const int MaxAwsAttributeNameLength = 256;

    /// <param name="provider">
    /// The service the draft is sent to; each service has its own identifier limits. Without it, a draft read from
    /// Kafka follows Kafka rules and any other draft follows Azure Service Bus, the strictest.
    /// </param>
    public static ValidationResult Validate(MessageDraft? draft, MessagingProvider? provider = null)
    {
        if (draft is null)
        {
            return new ValidationResult(
                [new ValidationError("message.required", "A message is required.")]);
        }

        var errors = new List<ValidationError>();
        ValidateBody(draft.Body, errors);
        var rules = provider ?? (draft.KafkaEnvelope is null ? MessagingProvider.AzureServiceBus : MessagingProvider.Kafka);
        ValidateBrokerProperties(draft.Properties, errors, rules);
        ValidateApplicationProperties(draft.ApplicationProperties, errors);
        if (draft.Properties is not null && draft.ApplicationProperties is not null)
        {
            ValidateProviderAttributes(draft.Properties, draft.ApplicationProperties, errors, rules,
                // Decoded: whitespace-only Base64 is zero bytes. Invalid Base64 is not "empty"; it has its own error above.
                draft.Body.TryGetBytes(out var bodyBytes) && bodyBytes.Length == 0);
        }
        // The total size, once everything it is computed from is known to be valid. Checked here, so the composer,
        // resends, scheduled resends and replays refuse an oversized message before anything is sent or scheduled.
        if (errors.Count == 0 && MessageSizeLimits.Check(draft, rules) is { } tooLarge)
        {
            errors.Add(new ValidationError("message.too_large", tooLarge, nameof(MessageDraft.Body)));
        }

        return errors.Count == 0 ? ValidationResult.Valid : new ValidationResult(errors);
    }

    private static void ValidateBody(
        EditableMessageBody? body,
        ICollection<ValidationError> errors)
    {
        if (body is null)
        {
            errors.Add(new ValidationError(
                "message.body.required",
                "A message body is required.",
                nameof(MessageDraft.Body)));
            return;
        }

        if (!Enum.IsDefined(body.Format))
        {
            errors.Add(new ValidationError(
                "message.body.format_invalid",
                "The message body format is not supported.",
                nameof(EditableMessageBody.Format)));
            return;
        }

        if (body.Content is null)
        {
            errors.Add(new ValidationError(
                "message.body.content_required",
                "Message body content cannot be null.",
                nameof(EditableMessageBody.Content)));
            return;
        }

        if (body.Format == MessageBodyFormat.Base64 && !body.TryGetBytes(out _))
        {
            errors.Add(new ValidationError(
                "message.body.base64_invalid",
                "The binary message body must be valid Base64.",
                nameof(EditableMessageBody.Content)));
        }

        if (body.Format == MessageBodyFormat.Json)
        {
            try
            {
                using var _ = JsonDocument.Parse(body.Content);
            }
            catch (JsonException)
            {
                errors.Add(new ValidationError(
                    "message.body.json_invalid",
                    "The message body must contain valid JSON.",
                    nameof(EditableMessageBody.Content)));
            }
        }
    }

    private static void ValidateBrokerProperties(
        EditableMessageProperties? properties,
        ICollection<ValidationError> errors,
        MessagingProvider rules)
    {
        if (properties is null)
        {
            errors.Add(new ValidationError(
                "message.properties.required",
                "Message properties are required.",
                nameof(MessageDraft.Properties)));
            return;
        }

        switch (rules)
        {
            case MessagingProvider.AzureServiceBus:
                // Service Bus caps every identifier at 128 characters.
                ValidateLength(properties.MessageId, nameof(properties.MessageId), errors);
                ValidateLength(properties.SessionId, nameof(properties.SessionId), errors);
                ValidateLength(properties.ReplyToSessionId, nameof(properties.ReplyToSessionId), errors);
                ValidateLength(properties.PartitionKey, nameof(properties.PartitionKey), errors);
                ValidateLength(properties.TransactionPartitionKey, nameof(properties.TransactionPartitionKey), errors);
                break;
            case MessagingProvider.AmazonSqsSns:
                // FIFO MessageDeduplicationId (MessageId) and MessageGroupId (SessionId, else PartitionKey): 128 characters.
                ValidateLength(properties.MessageId, nameof(properties.MessageId), errors, "Amazon SQS and SNS");
                ValidateLength(properties.SessionId, nameof(properties.SessionId), errors, "Amazon SQS and SNS");
                ValidateLength(properties.PartitionKey, nameof(properties.PartitionKey), errors, "Amazon SQS and SNS");
                break;
            case MessagingProvider.GooglePubSub:
                // The ordering key is the session ID, else the partition key. Pub/Sub assigns its own message ID.
                ValidateBytes(properties.SessionId, nameof(properties.SessionId), errors, MaxPubSubOrderingKeyBytes, "the Google Pub/Sub ordering key limit");
                ValidateBytes(properties.PartitionKey, nameof(properties.PartitionKey), errors, MaxPubSubOrderingKeyBytes, "the Google Pub/Sub ordering key limit");
                break;
            case MessagingProvider.RabbitMq:
                const string shortString = "the RabbitMQ (AMQP 0-9-1 short string) limit";
                ValidateBytes(properties.MessageId, nameof(properties.MessageId), errors, MaxAmqpShortStringBytes, shortString);
                ValidateBytes(properties.CorrelationId, nameof(properties.CorrelationId), errors, MaxAmqpShortStringBytes, shortString);
                ValidateBytes(properties.ContentType, nameof(properties.ContentType), errors, MaxAmqpShortStringBytes, shortString);
                ValidateBytes(properties.ReplyTo, nameof(properties.ReplyTo), errors, MaxAmqpShortStringBytes, shortString);
                ValidateBytes(properties.AmqpType, nameof(properties.AmqpType), errors, MaxAmqpShortStringBytes, shortString);
                ValidateBytes(properties.AmqpAppId, nameof(properties.AmqpAppId), errors, MaxAmqpShortStringBytes, shortString);
                ValidateBytes(properties.AmqpContentEncoding, nameof(properties.AmqpContentEncoding), errors, MaxAmqpShortStringBytes, shortString);
                // RabbitMQ refuses a per-message expiration above 10 years (rabbit_misc:check_expiry) and closes the channel.
                if (properties.TimeToLive is { } rabbitTtl && (long)rabbitTtl.TotalMilliseconds > MaxRabbitMqExpirationMilliseconds)
                {
                    errors.Add(new ValidationError(
                        "message.ttl.too_long",
                        $"RabbitMQ accepts a time to live of at most {MaxRabbitMqExpirationMilliseconds:N0} ms (10 years); " +
                        "shorten it or clear it.",
                        nameof(properties.TimeToLive)));
                }
                break;
        }

        if (properties.TimeToLive is { } timeToLive && timeToLive <= TimeSpan.Zero)
        {
            errors.Add(new ValidationError(
                "message.ttl.invalid",
                "Time to live must be greater than zero.",
                nameof(properties.TimeToLive)));
        }

        if (!string.IsNullOrEmpty(properties.SessionId) &&
            !string.IsNullOrEmpty(properties.PartitionKey) &&
            !string.Equals(properties.SessionId, properties.PartitionKey, StringComparison.Ordinal))
        {
            errors.Add(new ValidationError(
                "message.session_partition_mismatch",
                "When both Session ID and Partition Key are set, they must be identical.",
                nameof(properties.PartitionKey)));
        }
    }

    private static void ValidateLength(
        string? value,
        string memberName,
        ICollection<ValidationError> errors,
        string? service = null)
    {
        if (value?.Length > MaxMessageIdentifierLength)
        {
            errors.Add(new ValidationError(
                "message.identifier.too_long",
                service is null
                    ? $"{memberName} cannot exceed {MaxMessageIdentifierLength} characters."
                    : $"{memberName} cannot exceed {MaxMessageIdentifierLength} characters for {service}.",
                memberName));
        }
    }

    private static void ValidateBytes(
        string? value,
        string memberName,
        ICollection<ValidationError> errors,
        int maxBytes,
        string limitName)
    {
        if (value is not null && Encoding.UTF8.GetByteCount(value) > maxBytes)
        {
            errors.Add(new ValidationError(
                "message.identifier.too_long",
                $"{memberName} cannot exceed {maxBytes.ToString("N0", CultureInfo.InvariantCulture)} bytes of UTF-8 ({limitName}).",
                memberName));
        }
    }

    /// <summary>
    /// SQS and SNS attribute names: A-Z, a-z, 0-9, '_', '-' and '.', up to 256 characters, not starting or ending with
    /// a period and without periods in a row. Pub/Sub: at most 100 attributes, keys up to 256 bytes and values up to
    /// 1,024 bytes; the standard properties QueueLoom sends as attributes count too.
    /// </summary>
    private static void ValidateProviderAttributes(
        EditableMessageProperties properties,
        IReadOnlyList<MessageApplicationProperty> applicationProperties,
        ICollection<ValidationError> errors,
        MessagingProvider rules,
        bool draftBodyIsEmpty)
    {
        if (rules == MessagingProvider.AmazonSqsSns)
        {
            for (var index = 0; index < applicationProperties.Count; index++)
            {
                // A numeric property travels as an SQS/SNS Number attribute, which only takes a plain decimal number:
                // "1,000", "100-", "NaN" and "Infinity" are valid .NET values the service refuses.
                if (applicationProperties[index] is { Value: { } numberValue } numeric && numeric.Type != ApplicationPropertyType.String &&
                    MessageAttributeConventions.AwsDataType(numeric).StartsWith("Number", StringComparison.Ordinal) &&
                    !AwsNumber.IsMatch(numberValue))
                {
                    var label = numeric.Name is { Length: > 40 } longName ? TextLimits.Head(longName, 40) + "…" : numeric.Name;
                    errors.Add(new ValidationError(
                        "message.application_property.aws_number_invalid",
                        $"Amazon SQS and SNS send '{label}' as a Number attribute, which only accepts a plain decimal number " +
                        $"such as 1500 or -2.5e3; '{numberValue}' is refused. Enter it without separators, or send it as a String.",
                        $"{nameof(MessageDraft.ApplicationProperties)}[{index}]"));
                }
                if (applicationProperties[index]?.Name is not { Length: > 0 } name || IsAwsAttributeName(name))
                {
                    continue;
                }
                errors.Add(new ValidationError(
                    "message.application_property.name_invalid",
                    $"Amazon SQS and SNS do not accept the attribute name '{name}': use up to {MaxAwsAttributeNameLength} letters, digits, " +
                    "'_', '-' and '.', not starting or ending with '.' and without '..'.",
                    $"{nameof(MessageDraft.ApplicationProperties)}[{index}]"));
            }
        }
        else if (rules == MessagingProvider.GooglePubSub)
        {
            // The same map the publish builds: standard attributes first, then application properties, which replace a
            // standard attribute of the same name (ordinal names).
            var attributes = new Dictionary<string, string>(StringComparer.Ordinal);
            foreach (var (name, value) in MessageAttributeConventions.StandardAttributes(properties))
            {
                attributes[name] = value;
            }
            foreach (var property in applicationProperties.Where(property => property?.Name is not null))
            {
                attributes[property.Name] = property.Value ?? string.Empty;
            }
            if (PubSubPublishProblem(draftBodyIsEmpty, attributes.Keys) is { } problem)
            {
                errors.Add(new ValidationError("message.pubsub.unpublishable", problem, nameof(MessageDraft.ApplicationProperties)));
            }
            if (attributes.Count > MaxPubSubAttributes)
            {
                errors.Add(new ValidationError(
                    "message.application_properties.too_many",
                    $"Google Pub/Sub accepts at most {MaxPubSubAttributes} attributes per message; this message has {attributes.Count} " +
                    "(application properties plus correlation ID, subject, content type, reply-to and to).",
                    nameof(MessageDraft.ApplicationProperties)));
            }
            foreach (var (name, value) in attributes)
            {
                if (Encoding.UTF8.GetByteCount(name) > MaxPubSubAttributeKeyBytes ||
                    Encoding.UTF8.GetByteCount(value) > MaxPubSubAttributeValueBytes)
                {
                    errors.Add(new ValidationError(
                        "message.application_property.too_long",
                        $"Google Pub/Sub accepts attribute names up to {MaxPubSubAttributeKeyBytes} bytes and values up to " +
                        $"{MaxPubSubAttributeValueBytes.ToString("N0", CultureInfo.InvariantCulture)} bytes; " +
                        $"'{(name.Length > 40 ? TextLimits.Head(name, 40) + "…" : name)}' is longer.",
                        nameof(MessageDraft.ApplicationProperties)));
                }
            }
        }
    }

    /// <summary>
    /// Why Google Pub/Sub would refuse a publish with this data and these attribute keys, or null. A message needs non-empty
    /// data or at least one attribute, and attribute keys must be non-empty and must not begin with "goog" (any case).
    /// Refused before sending, such a message is a proven non-delivery, not an RPC failure of unknown outcome.
    /// </summary>
    public static string? PubSubPublishProblem(bool dataIsEmpty, IEnumerable<string> attributeKeys)
    {
        var keys = attributeKeys.ToArray();
        if (dataIsEmpty && keys.Length == 0)
        {
            return "Google Pub/Sub needs a message body or at least one attribute; this message has neither.";
        }
        var reserved = keys.FirstOrDefault(key => key.Length == 0 || key.StartsWith("goog", StringComparison.OrdinalIgnoreCase));
        return reserved is null
            ? null
            : reserved.Length == 0
                ? "Google Pub/Sub does not accept an attribute without a name."
                : $"Google Pub/Sub does not accept attribute names that begin with \"goog\" ('{(reserved.Length > 40 ? TextLimits.Head(reserved, 40) + "…" : reserved)}').";
    }

    /// <summary>Whether SQS and SNS accept <paramref name="name"/> as a message attribute name.</summary>
    public static bool IsAwsAttributeName(string name) =>
        name.Length is > 0 and <= MaxAwsAttributeNameLength && name[0] != '.' && name[^1] != '.' &&
        !name.Contains("..", StringComparison.Ordinal) &&
        name.All(character => char.IsAsciiLetterOrDigit(character) || character is '_' or '-' or '.');

    private static void ValidateApplicationProperties(
        IReadOnlyList<MessageApplicationProperty>? properties,
        ICollection<ValidationError> errors)
    {
        if (properties is null)
        {
            errors.Add(new ValidationError(
                "message.application_properties.required",
                "The application properties collection is required.",
                nameof(MessageDraft.ApplicationProperties)));
            return;
        }

        var names = new HashSet<string>(StringComparer.Ordinal);
        for (var index = 0; index < properties.Count; index++)
        {
            var property = properties[index];
            var memberName = $"{nameof(MessageDraft.ApplicationProperties)}[{index}]";

            if (property is null)
            {
                errors.Add(new ValidationError(
                    "message.application_property.required",
                    "An application property cannot be null.",
                    memberName));
                continue;
            }

            if (string.IsNullOrWhiteSpace(property.Name))
            {
                errors.Add(new ValidationError(
                    "message.application_property.name_required",
                    "An application property name is required.",
                    memberName));
            }
            else if (!names.Add(property.Name))
            {
                errors.Add(new ValidationError(
                    "message.application_property.duplicate",
                    $"The application property '{property.Name}' is duplicated.",
                    memberName));
            }

            if (!Enum.IsDefined(property.Type))
            {
                errors.Add(new ValidationError(
                    "message.application_property.type_invalid",
                    "The application property type is not supported.",
                    memberName));
                continue;
            }

            if (property.Value is null || !HasValidValue(property.Type, property.Value))
            {
                errors.Add(new ValidationError(
                    "message.application_property.value_invalid",
                    $"The value of '{property.Name}' is not a valid {property.Type}.",
                    memberName));
            }
            else if (property.WireType is not null && WireTypeError(property) is { } wireError)
            {
                errors.Add(new ValidationError("message.application_property.wire_type_invalid", wireError, memberName));
            }
        }
    }

    /// <summary>SQS and SNS: a message attribute's DataType (with its custom label) is at most 256 characters.</summary>
    public const int MaxAwsDataTypeLength = 256;

    private static readonly System.Text.RegularExpressions.Regex AwsNumber = new(
        @"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$",
        System.Text.RegularExpressions.RegexOptions.CultureInvariant);

    /// <summary>
    /// The wire type QueueLoom keeps is an SQS/SNS custom DataType label it has no type for ("String.Array",
    /// "Number.1"), and it is only sent while the property still has the type it was read with: "String.x" with
    /// String, "Number.x" with Int64, Decimal or a numeric String, "Binary.x" with Binary. A label that contradicts
    /// the type (typed into the raw editor, or kept after the type was changed there) would either be sent with a value the service refuses
    /// or be silently dropped while the draft and the routing preview still show it, so it is refused instead.
    /// </summary>
    private static string? WireTypeError(MessageApplicationProperty property)
    {
        var wire = property.WireType!;
        var name = property.Name.Length > 40 ? TextLimits.Head(property.Name, 40) + "…" : property.Name;
        if (wire == AmqpTypedValue.WireType)
        {
            // A RabbitMQ header kept with its exact AMQP types (a table, an array, a void value…): sent as it is.
            return property.Type != ApplicationPropertyType.String
                ? $"'{name}' has wireType '{wire}' but type {property.Type}: a typed AMQP value needs type String. Remove wireType to send it as {property.Type}."
                : AmqpTypedValue.Problem(property.Value) is { } problem
                    ? $"'{name}' is a typed AMQP value, but {problem}. Restore it, or remove wireType to send the text as it is."
                    : null;
        }
        var separator = wire.IndexOf('.', StringComparison.Ordinal);
        var prefix = separator > 0 ? wire[..separator] : wire;
        if (separator <= 0 || separator == wire.Length - 1 || prefix is not ("String" or "Number" or "Binary") ||
            wire.Length > MaxAwsDataTypeLength)
        {
            return $"'{name}' has wireType '{(wire.Length > 40 ? TextLimits.Head(wire, 40) + "…" : wire)}', which is not an Amazon SQS/SNS " +
                   $"custom type: use 'String.<label>', 'Number.<label>' or 'Binary.<label>' (up to {MaxAwsDataTypeLength} characters), " +
                   "or remove wireType.";
        }
        if (!HasAwsDataTypeCharacters(wire))
        {
            return $"'{name}' has a wireType containing a character Amazon SQS/SNS does not accept. " +
                   "Use valid Unicode text without forbidden control characters, or remove wireType.";
        }
        if (prefix == "String" && property.Type != ApplicationPropertyType.String)
        {
            return $"'{name}' has wireType '{wire}' but type {property.Type}: a String wire type needs type String. " +
                   $"Remove wireType to send it as {property.Type}, or set the type back to String.";
        }
        if (prefix == "Number" && property.Type is not (ApplicationPropertyType.Int64 or ApplicationPropertyType.Decimal or
                ApplicationPropertyType.String))
        {
            return $"'{name}' has wireType '{wire}' but type {property.Type}: a Number wire type needs type Int64, Decimal " +
                   $"or String. Remove wireType to send it as {property.Type}.";
        }
        if (prefix == "Binary" && property.Type != ApplicationPropertyType.Binary)
        {
            return $"'{name}' has wireType '{wire}' but type {property.Type}: a Binary wire type needs type Binary. " +
                   $"Remove wireType to send it as {property.Type}, or set the type back to Binary.";
        }
        if (prefix == "Number" && property.Type == ApplicationPropertyType.String && !AwsNumber.IsMatch(property.Value))
        {
            return $"'{name}' has wireType '{wire}' but its value is not a number, which Amazon SQS and SNS refuse for " +
                   "a Number attribute. Remove wireType to send it as text, or enter a number.";
        }
        return null;
    }

    // AWS DataType labels follow message-body Unicode rules, independently of attribute-name restrictions.
    private static bool HasAwsDataTypeCharacters(string value)
    {
        for (var index = 0; index < value.Length; index++)
        {
            var character = value[index];
            if (char.IsHighSurrogate(character))
            {
                if (++index == value.Length || !char.IsLowSurrogate(value[index])) return false;
                continue; // Every valid surrogate pair is in U+10000..U+10FFFF.
            }
            if (char.IsLowSurrogate(character) || character is '\ufffe' or '\uffff' ||
                character < ' ' && character is not ('\t' or '\n' or '\r')) return false;
        }
        return true;
    }

    private static bool HasValidValue(ApplicationPropertyType type, string value) => type switch
    {
        ApplicationPropertyType.String => true,
        ApplicationPropertyType.Boolean => bool.TryParse(value, out _),
        ApplicationPropertyType.Byte => byte.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out _),
        ApplicationPropertyType.SByte => sbyte.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out _),
        ApplicationPropertyType.Int16 => short.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out _),
        ApplicationPropertyType.UInt16 => ushort.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out _),
        ApplicationPropertyType.Int32 => int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out _),
        ApplicationPropertyType.UInt32 => uint.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out _),
        ApplicationPropertyType.Int64 => long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out _),
        ApplicationPropertyType.UInt64 => ulong.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out _),
        ApplicationPropertyType.Single => float.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out _),
        ApplicationPropertyType.Double => double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out _),
        ApplicationPropertyType.Decimal => decimal.TryParse(value, ApplicationPropertyValues.DecimalStyle, CultureInfo.InvariantCulture, out _),
        ApplicationPropertyType.Character => value.Length == 1,
        ApplicationPropertyType.Guid => Guid.TryParse(value, out _),
        ApplicationPropertyType.DateTime => DateTime.TryParse(
            value,
            CultureInfo.InvariantCulture,
            DateTimeStyles.RoundtripKind,
            out _),
        ApplicationPropertyType.DateTimeOffset => DateTimeOffset.TryParse(
            value,
            CultureInfo.InvariantCulture,
            DateTimeStyles.RoundtripKind,
            out _),
        ApplicationPropertyType.TimeSpan => TimeSpan.TryParse(value, CultureInfo.InvariantCulture, out _),
        ApplicationPropertyType.Uri => System.Uri.TryCreate(value, UriKind.RelativeOrAbsolute, out _),
        ApplicationPropertyType.Binary => IsBase64(value),
        _ => false
    };

    private static bool IsBase64(string value)
    {
        var buffer = new byte[(value.Length * 3 + 3) / 4];
        return Convert.TryFromBase64String(value, buffer, out _);
    }
}
