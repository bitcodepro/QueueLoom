using System.Buffers.Binary;
using System.Numerics;
using System.Text;
using System.Text.Json;

namespace QueueLoom.Infrastructure.RabbitMq;

// The management API's application/bert response is Erlang external term format (term_to_binary/1).
// Its void atom undefined is distinct from the binary text "undefined". JSON cannot retain that distinction.
// Decode only the data terms used by the binding resource; never execute or deserialize Erlang code terms.
internal sealed class RabbitMqBindingTerms(byte[] bytes)
{
    private int _position;
    private static readonly UTF8Encoding Utf8 = new(false, true);

    internal static IReadOnlyList<JsonElement> Decode(byte[] bytes)
    {
        var reader = new RabbitMqBindingTerms(bytes);
        if (reader.Byte() != 131) throw Invalid();
        var value = reader.Term(0);
        if (reader._position != bytes.Length || value is not object?[] bindings) throw Invalid();
        return bindings.Select(binding =>
        {
            if (binding is not Dictionary<string, object?>) throw Invalid();
            ValidateData(binding);
            return JsonSerializer.SerializeToElement(binding, RabbitMqBindingJson.Options);
        }).ToArray();
    }

    private static void ValidateData(object? value)
    {
        switch (value)
        {
            case ErlangTuple: throw Invalid();
            case Dictionary<string, object?> map: foreach (var item in map.Values) ValidateData(item); break;
            case object?[] array: foreach (var item in array) ValidateData(item); break;
        }
    }

    private object? Term(int depth)
    {
        if (depth > 64) throw Invalid();
        switch (Byte())
        {
            case 97: return (long)Byte(); // SMALL_INTEGER_EXT
            case 98: return (long)BinaryPrimitives.ReadInt32BigEndian(Take(4));
            case 70: return BinaryPrimitives.ReadDoubleBigEndian(Take(8));
            case 100: return Atom(Encoding.Latin1.GetString(Take(BinaryPrimitives.ReadUInt16BigEndian(Take(2)))));
            case 115: return Atom(Encoding.Latin1.GetString(Take(Byte())));
            case 118: return Atom(Utf8.GetString(Take(BinaryPrimitives.ReadUInt16BigEndian(Take(2)))));
            case 119: return Atom(Utf8.GetString(Take(Byte())));
            case 109: return Utf8.GetString(Take(Count())); // BINARY_EXT: the formatter already makes non-text binaries safe.
            case 110: return Big(Byte());
            case 111: return Big(Count());
            case 104: return new ErlangTuple(Tuple(Byte(), depth));
            case 105: return new ErlangTuple(Tuple(Count(), depth));
            case 106: return Array.Empty<object?>();
            case 107: return Take(BinaryPrimitives.ReadUInt16BigEndian(Take(2))).ToArray().Select(value => (object?)(long)value).ToArray();
            case 108:
                var list = Tuple(Count(), depth);
                if (Byte() != 106) throw Invalid(); // binding data contains proper lists only
                if (list.Length > 0 && list.All(item => item is ErlangTuple { Items.Length: 2 } pair && pair.Items[0] is string))
                {
                    var properties = new Dictionary<string, object?>(StringComparer.Ordinal);
                    foreach (var pair in list.Cast<ErlangTuple>())
                        if (!properties.TryAdd((string)pair.Items[0]!, pair.Items[1])) throw Invalid();
                    return properties;
                }
                return list;
            case 116:
                var count = Count();
                var map = new Dictionary<string, object?>(StringComparer.Ordinal);
                for (var index = 0; index < count; index++)
                {
                    if (Term(depth + 1) is not string key || !map.TryAdd(key, Term(depth + 1))) throw Invalid();
                }
                return map;
            default: throw Invalid();
        }
    }

    private object Big(int length)
    {
        var sign = Byte();
        if (sign > 1) throw Invalid();
        var value = new BigInteger(Take(length), isUnsigned: true, isBigEndian: false);
        if (sign == 1) value = -value;
        return value >= long.MinValue && value <= long.MaxValue ? (object)(long)value : value;
    }
    private object?[] Tuple(int count, int depth)
    {
        if (count > bytes.Length - _position) throw Invalid();
        var result = new object?[count];
        for (var index = 0; index < count; index++) result[index] = Term(depth + 1);
        return result;
    }
    private static object? Atom(string atom) => atom switch
    { "undefined" or "null" => null, "true" => true, "false" => false, _ => atom };
    private byte Byte() => Take(1)[0];
    private int Count()
    {
        var count = BinaryPrimitives.ReadUInt32BigEndian(Take(4));
        if (count > int.MaxValue || count > bytes.Length - _position) throw Invalid();
        return (int)count;
    }
    private ReadOnlySpan<byte> Take(int length)
    {
        if (length < 0 || length > bytes.Length - _position) throw Invalid();
        var result = bytes.AsSpan(_position, length);
        _position += length;
        return result;
    }
    private static InvalidOperationException Invalid() => new("RabbitMQ returned unsupported or malformed typed binding data. The binding was not replaced.");
    private sealed record ErlangTuple(object?[] Items);
}
