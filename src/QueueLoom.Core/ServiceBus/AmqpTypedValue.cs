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

    /// <summary>The field type tags, with how their "v" is written.</summary>
    public static readonly IReadOnlySet<string> Tags = new HashSet<string>(StringComparer.Ordinal)
    {
        "void", "bool", "i8", "u8", "i16", "u16", "i32", "u32", "i64", "f32", "f64", "dec", "ts", "longstr", "bytes",
        "array", "table"
    };

    /// <summary>Why the text is not a typed AMQP value; null when it is one.</summary>
    public static string? Problem(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return "it is empty";
        }
        try
        {
            using var document = JsonDocument.Parse(text);
            return Check(document.RootElement, 0);
        }
        catch (JsonException)
        {
            return "it is not JSON";
        }
    }

    private static string? Check(JsonElement value, int depth)
    {
        if (depth > 64)
        {
            return "it is nested too deeply";
        }
        if (value.ValueKind != JsonValueKind.Object || !value.TryGetProperty("t", out var tag) ||
            tag.ValueKind != JsonValueKind.String || !Tags.Contains(tag.GetString()!))
        {
            return "a field has no known type (\"t\")";
        }
        var type = tag.GetString()!;
        var hasValue = value.TryGetProperty("v", out var inner);
        return type switch
        {
            "void" => null,
            "array" => !hasValue || inner.ValueKind != JsonValueKind.Array
                ? "an array has no item list"
                : inner.EnumerateArray().Select(item => Check(item, depth + 1)).FirstOrDefault(problem => problem is not null),
            "table" => !hasValue || inner.ValueKind != JsonValueKind.Array
                ? "a table has no entry list"
                : inner.EnumerateArray().Select(entry =>
                        entry.ValueKind != JsonValueKind.Array || entry.GetArrayLength() != 2 ||
                        entry[0].ValueKind != JsonValueKind.String
                            ? "a table entry is not [name, value]"
                            : Check(entry[1], depth + 1))
                    .FirstOrDefault(problem => problem is not null),
            "bool" => hasValue && inner.ValueKind is JsonValueKind.True or JsonValueKind.False ? null : "a bool has no true/false",
            "longstr" or "bytes" => hasValue && inner.ValueKind == JsonValueKind.String && IsBase64(inner.GetString()!)
                ? null
                : $"a {type} value is not base64",
            _ => hasValue && inner.ValueKind == JsonValueKind.String ? null : $"a {type} value is missing"
        };
    }

    private static bool IsBase64(string text)
    {
        var buffer = new byte[(text.Length * 3 + 3) / 4];
        return Convert.TryFromBase64String(text, buffer, out _);
    }
}
