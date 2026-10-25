using System.Globalization;

namespace QueueLoom.Core.ServiceBus;

/// <summary>
/// Turns typed application-property values into the invariant text QueueLoom stores and back. Sending, backups and
/// routing checks all use this one conversion, so a Guid, a date or a float means the same everywhere.
/// </summary>
public static class ApplicationPropertyValues
{
    public static MessageApplicationProperty FromObject(string name, object? value)
    {
        var (type, text) = value switch
        {
            // Service Bus and AMQP carry a null value; it stays null instead of becoming empty text.
            null => (ApplicationPropertyType.Null, string.Empty),
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
            ReadOnlyMemory<byte> typed => (ApplicationPropertyType.Binary, Convert.ToBase64String(typed.Span)),
            _ => (ApplicationPropertyType.String, Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty)
        };
        return new MessageApplicationProperty(name, type, text);
    }

    /// <summary>
    /// What a send confirmation says when Binary properties go to Azure Service Bus: the service rejects byte[]
    /// application properties, so they arrive as their Base64 text. Null for any other destination or draft.
    /// </summary>
    public static string? AzureBinaryNotice(QueueLoom.Core.Profiles.MessagingProvider provider, IEnumerable<MessageApplicationProperty> properties)
    {
        ArgumentNullException.ThrowIfNull(properties);
        return provider == QueueLoom.Core.Profiles.MessagingProvider.AzureServiceBus &&
               properties.Any(property => property.Type == ApplicationPropertyType.Binary)
            ? "Binary application properties arrive as Base64 text: Azure Service Bus does not accept byte values."
            : null;
    }

    /// <summary>
    /// The value as an AMQP 0-9-1 (RabbitMQ) decimal carries it: a scale and a sign plus 31-bit magnitude, so the unscaled
    /// value must be at most 2,147,483,647. Trailing zeros are dropped when that makes it fit ("1.500000000000" is 1.5).
    /// Null when no exact form fits: RabbitMQ.Client throws for some such values and silently corrupts others
    /// (2147483648 is written as 0), so they are refused instead.
    /// </summary>
    public static decimal? ToAmqpDecimal(decimal value)
    {
        if (FitsAmqpDecimal(value))
        {
            return value;
        }
        var normalized = value / 1.000000000000000000000000000000000m;
        return FitsAmqpDecimal(normalized) ? normalized : null;
    }

    private static bool FitsAmqpDecimal(decimal value)
    {
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(value, bits);
        return bits[1] == 0 && bits[2] == 0 && bits[0] >= 0;
    }

    /// <summary>The typed value; throws <see cref="FormatException"/> when the text does not fit the type.</summary>
    /// <summary>The Decimal spellings QueueLoom accepts wherever it reads one, such as "1,000", "100-" and "1e3".</summary>
    public const NumberStyles DecimalStyle = NumberStyles.Number | NumberStyles.AllowExponent;

    public static object? ToObject(MessageApplicationProperty property)
    {
        ArgumentNullException.ThrowIfNull(property);
        try
        {
            return Parse(property);
        }
        catch (OverflowException exception)
        {
            // A number out of its type's range is a value that does not fit, like any other format error.
            throw new FormatException($"{property.Value} does not fit in a {property.Type}.", exception);
        }
    }

    private static object? Parse(MessageApplicationProperty property)
    {
        var value = property.Value;
        return property.Type switch
        {
            ApplicationPropertyType.String => value,
            ApplicationPropertyType.Boolean => bool.Parse(value),
            ApplicationPropertyType.Byte => byte.Parse(value, CultureInfo.InvariantCulture),
            ApplicationPropertyType.SByte => sbyte.Parse(value, CultureInfo.InvariantCulture),
            ApplicationPropertyType.Int16 => short.Parse(value, CultureInfo.InvariantCulture),
            ApplicationPropertyType.UInt16 => ushort.Parse(value, CultureInfo.InvariantCulture),
            ApplicationPropertyType.Int32 => int.Parse(value, CultureInfo.InvariantCulture),
            ApplicationPropertyType.UInt32 => uint.Parse(value, CultureInfo.InvariantCulture),
            ApplicationPropertyType.Int64 => long.Parse(value, CultureInfo.InvariantCulture),
            ApplicationPropertyType.UInt64 => ulong.Parse(value, CultureInfo.InvariantCulture),
            // Float, as the draft validator reads them: a comma is not a thousands separator, so "1,5" is refused, not 15.
            ApplicationPropertyType.Single => float.Parse(value, NumberStyles.Float, CultureInfo.InvariantCulture),
            ApplicationPropertyType.Double => double.Parse(value, NumberStyles.Float, CultureInfo.InvariantCulture),
            ApplicationPropertyType.Decimal => decimal.Parse(value, DecimalStyle, CultureInfo.InvariantCulture),
            ApplicationPropertyType.Character => value.Length == 1 ? value[0] : throw new FormatException("A character property holds exactly one character."),
            ApplicationPropertyType.Guid => Guid.Parse(value),
            ApplicationPropertyType.DateTime => DateTime.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            ApplicationPropertyType.DateTimeOffset => DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            ApplicationPropertyType.TimeSpan => TimeSpan.Parse(value, CultureInfo.InvariantCulture),
            ApplicationPropertyType.Uri => new Uri(value, UriKind.RelativeOrAbsolute),
            ApplicationPropertyType.Binary => Convert.FromBase64String(value),
            ApplicationPropertyType.Null => value.Length == 0 ? null : throw new FormatException("A null property has no value."),
            _ => throw new FormatException($"Unsupported application property type {property.Type}.")
        };
    }
}
