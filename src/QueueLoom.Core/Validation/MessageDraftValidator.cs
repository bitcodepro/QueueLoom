using System.Globalization;
using System.Text.Json;
using QueueLoom.Core.ServiceBus;

namespace QueueLoom.Core.Validation;

public static class MessageDraftValidator
{
    public const int MaxMessageIdentifierLength = 128;

    public static ValidationResult Validate(MessageDraft? draft)
    {
        if (draft is null)
        {
            return new ValidationResult(
                [new ValidationError("message.required", "A message is required.")]);
        }

        var errors = new List<ValidationError>();
        ValidateBody(draft.Body, errors);
        ValidateBrokerProperties(draft.Properties, errors);
        ValidateApplicationProperties(draft.ApplicationProperties, errors);

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
        ICollection<ValidationError> errors)
    {
        if (properties is null)
        {
            errors.Add(new ValidationError(
                "message.properties.required",
                "Message properties are required.",
                nameof(MessageDraft.Properties)));
            return;
        }

        ValidateLength(properties.MessageId, nameof(properties.MessageId), errors);
        ValidateLength(properties.SessionId, nameof(properties.SessionId), errors);
        ValidateLength(properties.ReplyToSessionId, nameof(properties.ReplyToSessionId), errors);
        ValidateLength(properties.PartitionKey, nameof(properties.PartitionKey), errors);
        ValidateLength(properties.TransactionPartitionKey, nameof(properties.TransactionPartitionKey), errors);

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
        ICollection<ValidationError> errors)
    {
        if (value?.Length > MaxMessageIdentifierLength)
        {
            errors.Add(new ValidationError(
                "message.identifier.too_long",
                $"{memberName} cannot exceed {MaxMessageIdentifierLength} characters.",
                memberName));
        }
    }

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
        }
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
        ApplicationPropertyType.Decimal => decimal.TryParse(value, NumberStyles.Number, CultureInfo.InvariantCulture, out _),
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
