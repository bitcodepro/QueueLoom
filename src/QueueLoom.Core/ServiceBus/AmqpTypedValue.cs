using QueueLoom.Core;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace QueueLoom.Core.ServiceBus;

/// <summary>
/// A RabbitMQ (AMQP 0-9-1) header value that the editable text types cannot carry exactly: a table, an array, a void
/// (null) value, a byte array field ('x'), a short or unsigned integer, a float or a timestamp outside the calendar.
/// It travels as a <see cref="MessageApplicationProperty"/> of type String with <see cref="WireType"/>, whose value is
/// JSON naming every field's AMQP type, so browsing, backups, schedules and resends give the broker back exactly the
/// structure and bytes it had: <c>{"t":"table","v":[["opaque",{"t":"bytes","v":"/w=="}],["n",{"t":"void"}]]}</c>.
/// Byte strings are base64, never decoded as text.
/// </summary>
public static class AmqpTypedValue
{
    /// <summary>The wire type label of such a property.</summary>
    public const string WireType = "AMQP";

    /// <summary>The deepest nesting of tables and arrays kept (far beyond what producers use).</summary>
    public const int MaximumNesting = 1_000;

    /// <summary>
    /// The JSON depth the typed form of <see cref="MaximumNesting"/> needs: a table adds three levels (its object, its
    /// entry list and the entry), so every reader and writer of this form uses this limit instead of the default 64.
    /// </summary>
    public const int MaximumJsonDepth = 3 * MaximumNesting + 8;

    /// <summary>Options for parsing the typed form.</summary>
    public static readonly JsonDocumentOptions ReadOptions = new() { MaxDepth = MaximumJsonDepth };

    /// <summary>Options for writing the typed form.</summary>
    public static readonly JsonSerializerOptions WriteOptions = new() { MaxDepth = MaximumJsonDepth };

    /// <summary>The field type tags.</summary>
    public static readonly IReadOnlySet<string> Tags = new HashSet<string>(StringComparer.Ordinal)
    {
        "void", "bool", "i8", "u8", "i16", "u16", "i32", "u32", "i64", "f32", "f64", "dec", "ts", "longstr", "bytes",
        "array", "table"
    };

    /// <summary>A table field name is an AMQP short string: at most 255 bytes of UTF-8.</summary>
    public const int MaximumFieldNameBytes = 255;

    /// <summary>
    /// Why the text is not a typed AMQP value that can be sent exactly as described; null when it is one. Numbers are
    /// checked against their field type's range, a decimal against what the wire carries (a scale and a 32-bit signed
    /// value), table field names for duplicates and length.
    /// </summary>
    public static string? Problem(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return "it is empty";
        }
        try
        {
            using var document = JsonDocument.Parse(text, ReadOptions);
            return Check(document.RootElement, 0);
        }
        catch (JsonException)
        {
            return "it is not JSON (or is nested too deeply)";
        }
    }

    private static string? Check(JsonElement value, int nesting)
    {
        if (nesting > MaximumNesting)
        {
            return $"tables and arrays are nested more than {MaximumNesting} levels deep";
        }
        if (value.ValueKind == JsonValueKind.Object &&
            value.EnumerateObject().GroupBy(member => member.Name, StringComparer.Ordinal).FirstOrDefault(group => group.Count() > 1) is { } repeated)
        {
            return $"a field repeats its \"{repeated.Key}\" member";
        }
        if (value.ValueKind != JsonValueKind.Object || !value.TryGetProperty("t", out var tag) ||
            tag.ValueKind != JsonValueKind.String || !Tags.Contains(tag.GetString()!))
        {
            return "a field has no known type (\"t\")";
        }
        var type = tag.GetString()!;
        var hasValue = value.TryGetProperty("v", out var inner);
        string? Text() => hasValue && inner.ValueKind == JsonValueKind.String ? inner.GetString() : null;
        var number = Text();
        return type switch
        {
            "void" => null,
            "bool" => hasValue && inner.ValueKind is JsonValueKind.True or JsonValueKind.False ? null : "a bool has no true/false",
            "i8" => Integer(number, sbyte.MinValue, sbyte.MaxValue, type),
            "u8" => Integer(number, byte.MinValue, byte.MaxValue, type),
            "i16" => Integer(number, short.MinValue, short.MaxValue, type),
            "u16" => Integer(number, ushort.MinValue, ushort.MaxValue, type),
            "i32" => Integer(number, int.MinValue, int.MaxValue, type),
            "u32" => Integer(number, uint.MinValue, uint.MaxValue, type),
            "i64" or "ts" => Integer(number, long.MinValue, long.MaxValue, type),
            // RabbitMQ (Erlang) has no NaN or infinity: a value that is not finite, or overflows the type, cannot be sent.
            "f32" => number is not null && float.TryParse(number, NumberStyles.Float, CultureInfo.InvariantCulture, out var single) &&
                     float.IsFinite(single) ? null : "an f32 value is not a finite 32-bit number",
            "f64" => number is not null && double.TryParse(number, NumberStyles.Float, CultureInfo.InvariantCulture, out var @double) &&
                     double.IsFinite(@double) ? null : "an f64 value is not a finite number",
            "dec" => Decimal(number),
            "longstr" or "bytes" => number is not null && IsBase64(number) ? null : $"a {type} value is not base64",
            "array" => !hasValue || inner.ValueKind != JsonValueKind.Array
                ? "an array has no item list"
                : inner.EnumerateArray().Select(item => Check(item, nesting + 1)).FirstOrDefault(problem => problem is not null),
            "table" => !hasValue || inner.ValueKind != JsonValueKind.Array ? "a table has no entry list" : Table(inner, nesting),
            _ => $"the type {type} is not known"
        };
    }

    private static string? Table(JsonElement entries, int nesting)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var entry in entries.EnumerateArray())
        {
            if (entry.ValueKind != JsonValueKind.Array || entry.GetArrayLength() != 2 || entry[0].ValueKind != JsonValueKind.String)
            {
                return "a table entry is not [name, value]";
            }
            var name = entry[0].GetString()!;
            if (!names.Add(name))
            {
                return $"the table field '{Short(name)}' appears twice";
            }
            if (Encoding.UTF8.GetByteCount(name) > MaximumFieldNameBytes)
            {
                return $"the table field name '{Short(name)}' is longer than {MaximumFieldNameBytes} bytes";
            }
            if (Check(entry[1], nesting + 1) is { } problem)
            {
                return problem;
            }
        }
        return null;
    }

    private static string? Integer(string? text, decimal minimum, decimal maximum, string type) =>
        text is not null && decimal.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var value) &&
        value >= minimum && value <= maximum
            ? null
            : $"an {type} value is not a whole number from {minimum} to {maximum}";

    /// <summary>AMQP carries a decimal as a scale (0-255) and a signed 32-bit value.</summary>
    private static string? Decimal(string? text)
    {
        if (text is null || !decimal.TryParse(text, NumberStyles.Number, CultureInfo.InvariantCulture, out var value))
        {
            return "a dec value is not a decimal number";
        }
        var bits = decimal.GetBits(value);
        return bits[1] == 0 && bits[2] == 0 && (uint)bits[0] <= int.MaxValue
            ? null
            : "a dec value has more digits than AMQP carries (a signed 32-bit value with a scale)";
    }

    private static string Short(string text) => text.Length > 40 ? TextLimits.Head(text, 40) + "…" : text;

    private static bool IsBase64(string text)
    {
        var buffer = new byte[(text.Length * 3 + 3) / 4];
        return Convert.TryFromBase64String(text, buffer, out _);
    }
}
