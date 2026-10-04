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

    /// <summary>The typed value; throws <see cref="FormatException"/> when the text does not fit the type.</summary>
    public static object ToObject(MessageApplicationProperty property)
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

    private static object Parse(MessageApplicationProperty property)
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
            ApplicationPropertyType.Decimal => decimal.Parse(value, NumberStyles.Float, CultureInfo.InvariantCulture),
            ApplicationPropertyType.Character => value.Length == 1 ? value[0] : throw new FormatException("A character property holds exactly one character."),
            ApplicationPropertyType.Guid => Guid.Parse(value),
            ApplicationPropertyType.DateTime => DateTime.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            ApplicationPropertyType.DateTimeOffset => DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            ApplicationPropertyType.TimeSpan => TimeSpan.Parse(value, CultureInfo.InvariantCulture),
            ApplicationPropertyType.Uri => new Uri(value, UriKind.RelativeOrAbsolute),
            ApplicationPropertyType.Binary => Convert.FromBase64String(value),
            _ => throw new FormatException($"Unsupported application property type {property.Type}.")
        };
    }
}
