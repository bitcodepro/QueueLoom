using System.Collections;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace QueueLoom.Core.ServiceBus;

/// <summary>
/// The lossless JSON form of the sender-set AMQP message annotations of an Azure Service Bus message
/// (<see cref="BrowsedMessage.AzureMessageAnnotations"/>), so schedules, replay plans, backups and exports send them
/// back with the same AMQP types. Each value is <c>{"type": "...", "value": ...}</c>: integers are JSON numbers;
/// floating point numbers, decimals, times and GUIDs are invariant text (so NaN and infinity survive); binary is base64;
/// lists and arrays hold typed items and maps typed <c>[key, value]</c> pairs, with the .NET element, key and value
/// types they had when those are scalar types listed here. An annotation whose value (or a nested value) has another
/// type is left out and named in the skipped list, so one odd value never fails the whole save.
/// </summary>
public static class AzureAnnotationJson
{
    /// <summary>The deepest nesting of lists and maps kept; deeper values are skipped, well inside JSON's depth limit.</summary>
    public const int MaximumNesting = 16;

    private static readonly Dictionary<Type, string> ScalarTags = new()
    {
        [typeof(bool)] = "bool", [typeof(byte)] = "byte", [typeof(sbyte)] = "sbyte", [typeof(short)] = "short",
        [typeof(ushort)] = "ushort", [typeof(int)] = "int", [typeof(uint)] = "uint", [typeof(long)] = "long",
        [typeof(ulong)] = "ulong", [typeof(float)] = "float", [typeof(double)] = "double", [typeof(decimal)] = "decimal",
        [typeof(char)] = "char", [typeof(string)] = "string", [typeof(Guid)] = "guid", [typeof(DateTime)] = "datetime",
        [typeof(DateTimeOffset)] = "datetimeoffset", [typeof(TimeSpan)] = "timespan", [typeof(Uri)] = "uri",
        [typeof(byte[])] = "binary"
    };

    private static readonly Dictionary<string, Type> ScalarTypes = ScalarTags.ToDictionary(pair => pair.Value, pair => pair.Key);

    /// <summary>
    /// The JSON form of the annotations (name → typed value), or null when there are none to keep. <paramref name="skipped"/>
    /// names each annotation left out and why.
    /// </summary>
    public static JsonObject? Encode(IReadOnlyDictionary<string, object?>? annotations, out IReadOnlyList<string> skipped)
    {
        var left = new List<string>();
        skipped = left;
        if (annotations is null || annotations.Count == 0) return null;
        var result = new JsonObject();
        foreach (var (name, value) in annotations)
        {
            try
            {
                result[name] = EncodeValue(value, 0);
            }
            catch (NotSupportedException exception)
            {
                left.Add($"{name}: {exception.Message}");
            }
        }
        return result.Count == 0 ? null : result;
    }

    /// <summary>The annotations read back from <see cref="Encode"/>'s form; null when absent. A damaged form throws
    /// <see cref="InvalidDataException"/>.</summary>
    public static IReadOnlyDictionary<string, object?>? Decode(JsonElement? encoded)
    {
        if (encoded is not { } element || element.ValueKind == JsonValueKind.Null) return null;
        if (element.ValueKind != JsonValueKind.Object) throw new InvalidDataException("Azure message annotations are not a JSON object.");
        var result = new Dictionary<string, object?>(StringComparer.Ordinal);
        try
        {
            foreach (var member in element.EnumerateObject())
            {
                if (!result.TryAdd(member.Name, DecodeValue(member.Value, 0)))
                    throw new InvalidDataException($"The Azure message annotation '{member.Name}' appears twice.");
            }
        }
        catch (Exception exception) when (exception is FormatException or OverflowException or ArgumentException or InvalidOperationException
                                              or InvalidCastException or KeyNotFoundException)
        {
            throw new InvalidDataException($"An Azure message annotation cannot be read: {exception.Message}", exception);
        }
        return result.Count == 0 ? null : result;
    }

    /// <inheritdoc cref="Decode(JsonElement?)"/>
    public static IReadOnlyDictionary<string, object?>? Decode(JsonObject? encoded) =>
        encoded is null ? null : Decode(JsonSerializer.SerializeToElement(encoded));

    private static JsonObject EncodeValue(object? value, int nesting)
    {
        if (nesting > MaximumNesting) throw new NotSupportedException($"lists and maps are nested more than {MaximumNesting} levels deep");
        JsonObject Typed(string type, JsonNode? node) => new() { ["type"] = type, ["value"] = node };
        string Text(IFormattable formattable, string? format) => formattable.ToString(format, CultureInfo.InvariantCulture);
        switch (value)
        {
            case null: return new JsonObject { ["type"] = "null" };
            case bool flag: return Typed("bool", flag);
            case byte number: return Typed("byte", number);
            case sbyte number: return Typed("sbyte", number);
            case short number: return Typed("short", number);
            case ushort number: return Typed("ushort", number);
            case int number: return Typed("int", number);
            case uint number: return Typed("uint", number);
            case long number: return Typed("long", number);
            case ulong number: return Typed("ulong", number);
            case float number: return Typed("float", Text(number, "R"));
            case double number: return Typed("double", Text(number, "R"));
            case decimal number: return Typed("decimal", Text(number, null));
            case char character: return Typed("char", character.ToString());
            case string text: return Typed("string", text);
            case Guid guid: return Typed("guid", guid.ToString("D"));
            case DateTime time: return Typed("datetime", Text(time, "O"));
            case DateTimeOffset time: return Typed("datetimeoffset", Text(time, "O"));
            case TimeSpan span: return Typed("timespan", Text(span, "c"));
            case Uri uri: return Typed("uri", uri.OriginalString);
            case byte[] bytes: return Typed("binary", Convert.ToBase64String(bytes));
            // The AMQP library hands binary over as a segment or memory; it is sent back as the same bytes.
            case ArraySegment<byte> segment: return Typed("binary", Convert.ToBase64String(segment.AsSpan()));
            case ReadOnlyMemory<byte> memory: return Typed("binary", Convert.ToBase64String(memory.Span));
            case Memory<byte> memory: return Typed("binary", Convert.ToBase64String(memory.Span));
            case IDictionary map:
            {
                var entries = new JsonArray();
                foreach (DictionaryEntry entry in map)
                {
                    entries.Add(new JsonArray(EncodeValue(entry.Key, nesting + 1), EncodeValue(entry.Value, nesting + 1)));
                }
                var result = Typed("map", entries);
                var arguments = map.GetType().IsGenericType ? map.GetType().GetGenericArguments() : [];
                if (arguments.Length == 2 && ScalarTags.TryGetValue(arguments[0], out var keyType)) result["keyType"] = keyType;
                if (arguments.Length == 2 && ScalarTags.TryGetValue(arguments[1], out var valueType)) result["valueType"] = valueType;
                return result;
            }
            case Array array when array.Rank == 1:
            {
                var result = Typed("array", Items(array, nesting));
                if (ScalarTags.TryGetValue(array.GetType().GetElementType()!, out var elementType)) result["elementType"] = elementType;
                return result;
            }
            case IList list:
            {
                var result = Typed("list", Items(list, nesting));
                var arguments = list.GetType().IsGenericType ? list.GetType().GetGenericArguments() : [];
                if (arguments.Length == 1 && ScalarTags.TryGetValue(arguments[0], out var elementType)) result["elementType"] = elementType;
                return result;
            }
            default: throw new NotSupportedException($"its type {value.GetType().FullName} has no lossless form");
        }
    }

    private static JsonArray Items(IEnumerable items, int nesting)
    {
        var result = new JsonArray();
        foreach (var item in items) result.Add(EncodeValue(item, nesting + 1));
        return result;
    }

    private static object? DecodeValue(JsonElement element, int nesting)
    {
        if (nesting > MaximumNesting) throw new FormatException($"lists and maps are nested more than {MaximumNesting} levels deep");
        if (element.ValueKind != JsonValueKind.Object || !element.TryGetProperty("type", out var tag) || tag.ValueKind != JsonValueKind.String)
            throw new FormatException("a value has no type");
        var type = tag.GetString()!;
        if (type == "null") return null;
        if (!element.TryGetProperty("value", out var value)) throw new FormatException($"a {type} has no value");
        string Text() => value.GetString() ?? throw new FormatException($"a {type} value is null");
        var invariant = CultureInfo.InvariantCulture;
        return type switch
        {
            "bool" => value.GetBoolean(),
            "byte" => value.GetByte(),
            "sbyte" => value.GetSByte(),
            "short" => value.GetInt16(),
            "ushort" => value.GetUInt16(),
            "int" => value.GetInt32(),
            "uint" => value.GetUInt32(),
            "long" => value.GetInt64(),
            "ulong" => value.GetUInt64(),
            "float" => float.Parse(Text(), NumberStyles.Float, invariant),
            "double" => double.Parse(Text(), NumberStyles.Float, invariant),
            "decimal" => decimal.Parse(Text(), NumberStyles.Number, invariant),
            "char" => Text() is { Length: 1 } character ? character[0] : throw new FormatException("a char value is not one character"),
            "string" => Text(),
            "guid" => Guid.ParseExact(Text(), "D"),
            "datetime" => DateTime.Parse(Text(), invariant, DateTimeStyles.RoundtripKind),
            "datetimeoffset" => DateTimeOffset.Parse(Text(), invariant, DateTimeStyles.RoundtripKind),
            "timespan" => TimeSpan.ParseExact(Text(), "c", invariant),
            "uri" => new Uri(Text(), UriKind.RelativeOrAbsolute),
            "binary" => Convert.FromBase64String(Text()),
            "list" => Fill((IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(Optional(element, "elementType")))!, value, nesting),
            "array" => ToArray(Optional(element, "elementType"), value, nesting),
            "map" => Map(element, value, nesting),
            _ => throw new FormatException($"the type {type} is not known")
        };
    }

    private static Type Optional(JsonElement element, string name) =>
        !element.TryGetProperty(name, out var tag) ? typeof(object)
        : tag.ValueKind == JsonValueKind.String && ScalarTypes.TryGetValue(tag.GetString()!, out var type) ? type
        : throw new FormatException($"the {name} is not known");

    private static IList Fill(IList list, JsonElement items, int nesting)
    {
        foreach (var item in items.EnumerateArray()) list.Add(DecodeValue(item, nesting + 1));
        return list;
    }

    private static Array ToArray(Type elementType, JsonElement items, int nesting)
    {
        var array = Array.CreateInstance(elementType, items.GetArrayLength());
        var index = 0;
        foreach (var item in items.EnumerateArray()) array.SetValue(DecodeValue(item, nesting + 1), index++);
        return array;
    }

    private static IDictionary Map(JsonElement element, JsonElement entries, int nesting)
    {
        var map = (IDictionary)Activator.CreateInstance(
            typeof(Dictionary<,>).MakeGenericType(Optional(element, "keyType"), Optional(element, "valueType")))!;
        foreach (var entry in entries.EnumerateArray())
        {
            if (entry.ValueKind != JsonValueKind.Array || entry.GetArrayLength() != 2) throw new FormatException("a map entry is not [key, value]");
            var key = DecodeValue(entry[0], nesting + 1) ?? throw new FormatException("a map key is null");
            map.Add(key, DecodeValue(entry[1], nesting + 1));
        }
        return map;
    }
}
