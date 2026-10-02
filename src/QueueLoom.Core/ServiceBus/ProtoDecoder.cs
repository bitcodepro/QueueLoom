using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace QueueLoom.Core.ServiceBus;

/// <summary>
/// Decodes Protobuf bodies with their message type: field names, enum names, nested messages, repeated (also packed)
/// fields and maps. Fields the type does not declare are kept under their number, such as "#7".
/// </summary>
public static class ProtoDecoder
{
    private const int MaximumDepth = 32;

    private static readonly JsonWriterOptions Indented = new() { Indented = true, Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };

    private static readonly UTF8Encoding StrictUtf8 = new(false, throwOnInvalidBytes: true);

    /// <summary>
    /// The body as JSON, and how well it fits the type: 1 when every field is declared with a matching wire type,
    /// lower for each field that is not; null when the bytes are not Protobuf at all.
    /// </summary>
    public static (string Json, double Fit)? Decode(ReadOnlySpan<byte> body, ProtoMessageType type, ProtoSchemaSet schemas)
    {
        ArgumentNullException.ThrowIfNull(type);
        ArgumentNullException.ThrowIfNull(schemas);
        var stats = new Stats();
        object? tree;
        try
        {
            tree = ReadMessage(body.ToArray(), type, schemas, stats, 0);
        }
        catch (InvalidDataException)
        {
            return null;
        }
        if (tree is null)
        {
            return null;
        }
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, Indented))
        {
            Write(writer, tree);
        }
        return (Encoding.UTF8.GetString(stream.ToArray()), stats.Total == 0 ? 1 : (double)stats.Matched / stats.Total);
    }

    /// <summary>
    /// The type that fits the body best: <paramref name="hint"/> when it names a known type, else every type is tried
    /// and one that fits exactly is taken (the one with the most fields set). Null when none fits exactly.
    /// </summary>
    public static (ProtoMessageType Type, string Json)? DecodeBestFit(ReadOnlySpan<byte> body, ProtoSchemaSet schemas, string? hint = null)
    {
        ArgumentNullException.ThrowIfNull(schemas);
        if (schemas.Resolve(hint) is { } hinted && Decode(body, hinted, schemas) is { } decoded)
        {
            return (hinted, decoded.Json);
        }

        (ProtoMessageType Type, string Json, int Fields)? best = null;
        var copy = body.ToArray();
        foreach (var type in schemas.Messages)
        {
            if (Decode(copy, type, schemas) is not { Fit: >= 1 } candidate)
            {
                continue;
            }
            var fields = CountFields(copy, type);
            if (best is null || fields > best.Value.Fields)
            {
                best = (type, candidate.Json, fields);
            }
        }
        return best is { } found ? (found.Type, found.Json) : null;
    }

    /// <summary>How many declared fields a fitting body sets, to prefer the type that explains the most of it.</summary>
    private static int CountFields(byte[] body, ProtoMessageType type)
    {
        var numbers = new HashSet<int>();
        var position = 0;
        while (position < body.Length && TryReadTag(body, ref position, out var number, out var wireType) && Skip(body, ref position, wireType))
        {
            if (type.FieldsByNumber.ContainsKey(number))
            {
                numbers.Add(number);
            }
        }
        return numbers.Count;
    }

    private sealed class Stats
    {
        public int Total;
        public int Matched;
    }

    /// <summary>A decoded message: field name to value, list or nested message, in field-number order.</summary>
    private sealed class Node : SortedDictionary<int, (string Name, List<object?> Values, bool Repeated)>
    {
        // Keep the wire's order for embedded-message merging. A final dictionary loses
        // a oneof transition (a -> b -> a), which must clear an earlier a even when the
        // final later member is a again. Entries hold immutable decoded occurrences.
        public List<Occurrence> Occurrences { get; } = [];
        public Dictionary<string, int> Oneofs { get; } = new(StringComparer.Ordinal);
        /// <summary>An entry of a map field: written as "key": value of the map's object.</summary>
        public bool IsMapEntry { get; init; }
    }

    private sealed record Occurrence(int Number, ProtoField? Field, object?[] Values, bool Matched);

    private static Node? ReadMessage(byte[] data, ProtoMessageType type, ProtoSchemaSet schemas, Stats stats, int depth)
    {
        if (depth > MaximumDepth)
        {
            throw new InvalidDataException("Nested too deeply.");
        }
        var node = new Node { IsMapEntry = type.IsMapEntry };
        var position = 0;
        while (position < data.Length)
        {
            if (!TryReadTag(data, ref position, out var number, out var wireType) || number <= 0)
            {
                throw new InvalidDataException("Not a Protobuf message.");
            }
            stats.Total++;
            type.FieldsByNumber.TryGetValue(number, out var field);
            object? value;
            var matched = field is not null && Fits(field, wireType);
            switch (wireType)
            {
                case 0:
                    var varint = ReadVarint(data, ref position);
                    value = matched ? FromVarint(field!, varint, schemas) : (long)varint;
                    break;
                case 1:
                    Require(data, position, 8);
                    var fixed64 = BinaryPrimitives.ReadUInt64LittleEndian(data.AsSpan(position, 8));
                    position += 8;
                    value = matched ? field!.Type switch
                    {
                        ProtoFieldType.Double => (object)BitConverter.UInt64BitsToDouble(fixed64),
                        ProtoFieldType.SFixed64 => (long)fixed64,
                        _ => fixed64
                    } : fixed64;
                    break;
                case 5:
                    Require(data, position, 4);
                    var fixed32 = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(position, 4));
                    position += 4;
                    value = matched ? field!.Type switch
                    {
                        ProtoFieldType.Float => (object)(double)BitConverter.UInt32BitsToSingle(fixed32),
                        ProtoFieldType.SFixed32 => (int)fixed32,
                        _ => fixed32
                    } : fixed32;
                    break;
                case 2:
                    var length = ReadVarint(data, ref position);
                    if (length > (ulong)(data.Length - position))
                    {
                        throw new InvalidDataException("A length runs past the end.");
                    }
                    var bytes = data[position..(position + (int)length)];
                    position += (int)length;
                    if (field is null)
                    {
                        value = Convert.ToBase64String(bytes);
                        break;
                    }
                    if (field.Type == ProtoFieldType.String)
                    {
                        try
                        {
                            value = StrictUtf8.GetString(bytes);
                        }
                        catch (DecoderFallbackException)
                        {
                            matched = false;
                            value = Convert.ToBase64String(bytes);
                        }
                    }
                    else if (field.Type == ProtoFieldType.Bytes)
                    {
                        value = Convert.ToBase64String(bytes);
                    }
                    else if (field.Type == ProtoFieldType.Message)
                    {
                        if (field.TypeName is not null && schemas.FindMessage(field.TypeName) is { } nested)
                        {
                            var inner = new Stats();
                            try
                            {
                                value = ReadMessage(bytes, nested, schemas, inner, depth + 1);
                            }
                            catch (InvalidDataException)
                            {
                                inner.Total++;
                                value = Convert.ToBase64String(bytes);
                            }
                            stats.Total += inner.Total;
                            stats.Matched += inner.Matched;
                        }
                        else
                        {
                            // An imported type that was not loaded: the bytes stay as they are.
                            value = Convert.ToBase64String(bytes);
                        }
                    }
                    else if (field.IsRepeated && IsPackable(field.Type))
                    {
                        var items = ReadPacked(bytes, field, schemas);
                        if (items is null)
                        {
                            matched = false;
                            value = Convert.ToBase64String(bytes);
                        }
                        else
                        {
                            if (matched)
                            {
                                stats.Matched++;
                            }
                            Add(node, number, field, items, matched);
                            continue;
                        }
                    }
                    else
                    {
                        matched = false;
                        value = Convert.ToBase64String(bytes);
                    }
                    break;
                default:
                    throw new InvalidDataException($"Wire type {wireType} is not used in proto3.");
            }
            if (matched)
            {
                stats.Matched++;
            }
            Add(node, number, matched ? field : null, [value], matched);
        }
        return node;
    }

    /// <summary>
    /// Adds what the wire holds for a field. A field the schema declares singular may appear more than once: the last
    /// scalar wins and embedded messages merge, as Protobuf parsers do. Repeated and undeclared fields collect values.
    /// </summary>
    private static void Add(Node node, int number, ProtoField? field, IEnumerable<object?> values, bool matched)
    {
        var occurrence = new Occurrence(number, field, values.ToArray(), matched);
        node.Occurrences.Add(occurrence);
        Apply(node, occurrence);
    }

    private static void Apply(Node node, Occurrence occurrence)
    {
        var (number, field, values, matched) = occurrence;
        if (matched && field?.Oneof is { } oneof)
        {
            if (node.Oneofs.TryGetValue(oneof, out var previous) && previous != number)
                node.Remove(previous);
            node.Oneofs[oneof] = number;
        }
        var name = matched && field is not null ? field.Name : $"#{number}";
        var singular = matched && field is { IsRepeated: false };
        if (!node.TryGetValue(number, out var entry))
        {
            entry = (name, [], field?.IsRepeated == true);
            node[number] = entry;
        }
        if (singular)
        {
            foreach (var value in values)
            {
                if (entry.Values.Count == 1 && entry.Values[0] is Node earlier && value is Node later)
                {
                    Merge(earlier, later);
                }
                else
                {
                    entry.Values.Clear();
                    entry.Values.Add(value is Node nested ? Copy(nested) : value);
                }
            }
            node[number] = (entry.Name, entry.Values, false);
            return;
        }
        entry.Values.AddRange(values.Select(value => value is Node nested ? Copy(nested) : value));
        node[number] = (entry.Name, entry.Values, entry.Repeated || entry.Values.Count > 1);
    }

    /// <summary>Merges a later occurrence of a singular embedded message into the earlier one.</summary>
    private static void Merge(Node target, Node source)
    {
        foreach (var occurrence in source.Occurrences)
            Add(target, occurrence.Number, occurrence.Field, occurrence.Values, occurrence.Matched);
    }

    private static Node Copy(Node source)
    {
        var copy = new Node { IsMapEntry = source.IsMapEntry };
        Merge(copy, source);
        return copy;
    }

    private static bool Fits(ProtoField field, int wireType) => (field.Type, wireType) switch
    {
        (ProtoFieldType.Int32 or ProtoFieldType.Int64 or ProtoFieldType.UInt32 or ProtoFieldType.UInt64 or ProtoFieldType.SInt32 or
            ProtoFieldType.SInt64 or ProtoFieldType.Bool or ProtoFieldType.Enum, 0) => true,
        (ProtoFieldType.Fixed64 or ProtoFieldType.SFixed64 or ProtoFieldType.Double, 1) => true,
        (ProtoFieldType.Fixed32 or ProtoFieldType.SFixed32 or ProtoFieldType.Float, 5) => true,
        (ProtoFieldType.String or ProtoFieldType.Bytes or ProtoFieldType.Message, 2) => true,
        (_, 2) => field.IsRepeated && IsPackable(field.Type),
        _ => false
    };

    private static bool IsPackable(ProtoFieldType type) => type is not (ProtoFieldType.String or ProtoFieldType.Bytes or ProtoFieldType.Message or ProtoFieldType.Group);

    private static List<object?>? ReadPacked(byte[] bytes, ProtoField field, ProtoSchemaSet schemas)
    {
        var items = new List<object?>();
        var position = 0;
        try
        {
            while (position < bytes.Length)
            {
                switch (field.Type)
                {
                    case ProtoFieldType.Double or ProtoFieldType.Fixed64 or ProtoFieldType.SFixed64:
                        Require(bytes, position, 8);
                        var wide = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(position, 8));
                        // Each kind is boxed as itself: a shared conditional would turn the integers into doubles.
                        items.Add(field.Type switch
                        {
                            ProtoFieldType.Double => (object)BitConverter.UInt64BitsToDouble(wide),
                            ProtoFieldType.SFixed64 => (long)wide,
                            _ => wide
                        });
                        position += 8;
                        break;
                    case ProtoFieldType.Float or ProtoFieldType.Fixed32 or ProtoFieldType.SFixed32:
                        Require(bytes, position, 4);
                        var narrow = BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(position, 4));
                        items.Add(field.Type switch
                        {
                            ProtoFieldType.Float => (object)(double)BitConverter.UInt32BitsToSingle(narrow),
                            ProtoFieldType.SFixed32 => (int)narrow,
                            _ => narrow
                        });
                        position += 4;
                        break;
                    default:
                        items.Add(FromVarint(field, ReadVarint(bytes, ref position), schemas));
                        break;
                }
            }
        }
        catch (InvalidDataException)
        {
            return null;
        }
        return items;
    }

    private static object? FromVarint(ProtoField field, ulong value, ProtoSchemaSet schemas) => field.Type switch
    {
        ProtoFieldType.Bool => value != 0,
        ProtoFieldType.Int32 => (int)value,
        ProtoFieldType.Int64 => (long)value,
        ProtoFieldType.UInt32 => (uint)value,
        ProtoFieldType.SInt32 or ProtoFieldType.SInt64 => (long)(value >> 1) ^ -(long)(value & 1),
        ProtoFieldType.Enum => field.TypeName is not null && schemas.FindEnum(field.TypeName) is { } type &&
                               type.Values.TryGetValue((int)value, out var name)
            ? name
            : (int)value,
        _ => value
    };

    private static void Write(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null:
                writer.WriteNullValue();
                break;
            case Node node:
                writer.WriteStartObject();
                foreach (var (_, (name, values, repeated)) in node)
                {
                    writer.WritePropertyName(name);
                    if (repeated && values.Count > 0 && values.All(item => item is Node { IsMapEntry: true }))
                    {
                        writer.WriteStartObject();
                        foreach (Node entry in values.Cast<Node>())
                        {
                            writer.WritePropertyName(entry.TryGetValue(1, out var key) ? KeyText(key.Values.LastOrDefault()) : string.Empty);
                            Write(writer, entry.TryGetValue(2, out var item) ? item.Values.LastOrDefault() : null);
                        }
                        writer.WriteEndObject();
                    }
                    else if (repeated)
                    {
                        writer.WriteStartArray();
                        foreach (var item in values)
                        {
                            Write(writer, item);
                        }
                        writer.WriteEndArray();
                    }
                    else
                    {
                        Write(writer, values[^1]);
                    }
                }
                writer.WriteEndObject();
                break;
            case string text:
                writer.WriteStringValue(text);
                break;
            case bool flag:
                writer.WriteBooleanValue(flag);
                break;
            case int number:
                writer.WriteNumberValue(number);
                break;
            case uint number:
                writer.WriteNumberValue(number);
                break;
            case long number:
                writer.WriteNumberValue(number);
                break;
            case ulong number:
                writer.WriteNumberValue(number);
                break;
            case double number when double.IsFinite(number):
                writer.WriteNumberValue(number);
                break;
            case double number:
                writer.WriteStringValue(number.ToString(CultureInfo.InvariantCulture));
                break;
            default:
                writer.WriteStringValue(Convert.ToString(value, CultureInfo.InvariantCulture));
                break;
        }
    }

    private static string KeyText(object? key) => key switch
    {
        bool flag => flag ? "true" : "false",
        null => string.Empty,
        _ => Convert.ToString(key, CultureInfo.InvariantCulture) ?? string.Empty
    };

    private static bool TryReadTag(byte[] data, ref int position, out int number, out int wireType)
    {
        number = 0;
        wireType = 0;
        try
        {
            var tag = ReadVarint(data, ref position);
            if (tag >> 3 is 0 or > 536_870_911)
            {
                return false;
            }
            number = (int)(tag >> 3);
            wireType = (int)(tag & 7);
            return true;
        }
        catch (InvalidDataException)
        {
            return false;
        }
    }

    private static bool Skip(byte[] data, ref int position, int wireType)
    {
        try
        {
            switch (wireType)
            {
                case 0:
                    ReadVarint(data, ref position);
                    return true;
                case 1:
                    position += 8;
                    return position <= data.Length;
                case 5:
                    position += 4;
                    return position <= data.Length;
                case 2:
                    var length = ReadVarint(data, ref position);
                    if (length > (ulong)(data.Length - position))
                    {
                        return false;
                    }
                    position += (int)length;
                    return true;
                default:
                    return false;
            }
        }
        catch (InvalidDataException)
        {
            return false;
        }
    }

    private static void Require(byte[] data, int position, int count)
    {
        if (position + count > data.Length)
        {
            throw new InvalidDataException("A field runs past the end.");
        }
    }

    private static ulong ReadVarint(byte[] data, ref int position)
    {
        ulong value = 0;
        for (var shift = 0; shift < 64; shift += 7)
        {
            if (position >= data.Length)
            {
                throw new InvalidDataException("A number runs past the end.");
            }
            var current = data[position++];
            value |= (ulong)(current & 0x7F) << shift;
            if ((current & 0x80) == 0)
            {
                return value;
            }
        }
        throw new InvalidDataException("A number is too long.");
    }
}
