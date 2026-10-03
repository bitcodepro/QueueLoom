using System.Globalization;
using System.Numerics;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace QueueLoom.Infrastructure.RabbitMq;

// RabbitMQ headers compare integer and floating values differently. General JSON numeric equality and
// the default serialization of a whole-valued double both erase this distinction.
internal static class RabbitMqBindingJson
{
    internal static JsonSerializerOptions Options { get; } = new()
    {
        Converters = { new FloatingConverter(), new SingleConverter(), new BigIntegerConverter() }
    };

    internal static bool Equal(JsonElement left, JsonElement right)
    {
        if (left.ValueKind != right.ValueKind) return false;
        switch (left.ValueKind)
        {
            case JsonValueKind.Null or JsonValueKind.True or JsonValueKind.False: return true;
            case JsonValueKind.String: return string.Equals(left.GetString(), right.GetString(), StringComparison.Ordinal);
            case JsonValueKind.Number:
                var a = left.GetRawText();
                var b = right.GetRawText();
                var floating = a.IndexOfAny(['.', 'e', 'E']) >= 0;
                if (floating != (b.IndexOfAny(['.', 'e', 'E']) >= 0)) return false;
                return floating
                    ? left.TryGetDouble(out var x) && right.TryGetDouble(out var y) && double.IsFinite(x) && double.IsFinite(y) &&
                        BitConverter.DoubleToInt64Bits(x) == BitConverter.DoubleToInt64Bits(y)
                    : BigInteger.Parse(a, CultureInfo.InvariantCulture) == BigInteger.Parse(b, CultureInfo.InvariantCulture);
            case JsonValueKind.Array:
                return left.GetArrayLength() == right.GetArrayLength() &&
                    Enumerable.Range(0, left.GetArrayLength()).All(index => Equal(left[index], right[index]));
            case JsonValueKind.Object:
                var fields = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
                foreach (var field in right.EnumerateObject()) if (!fields.TryAdd(field.Name, field.Value)) return false;
                foreach (var field in left.EnumerateObject())
                    if (!fields.Remove(field.Name, out var value) || !Equal(field.Value, value)) return false;
                return fields.Count == 0;
            default: return false;
        }
    }

    private static void WriteFloating(Utf8JsonWriter writer, double value)
    {
        if (!double.IsFinite(value)) throw new JsonException("RabbitMQ binding numbers must be finite.");
        var text = value.ToString("R", CultureInfo.InvariantCulture);
        writer.WriteRawValue(text.IndexOfAny(['.', 'e', 'E']) < 0 ? text + ".0" : text);
    }
    private sealed class FloatingConverter : JsonConverter<double>
    {
        public override double Read(ref Utf8JsonReader reader, Type type, JsonSerializerOptions options) => reader.GetDouble();
        public override void Write(Utf8JsonWriter writer, double value, JsonSerializerOptions options) => WriteFloating(writer, value);
    }
    private sealed class SingleConverter : JsonConverter<float>
    {
        public override float Read(ref Utf8JsonReader reader, Type type, JsonSerializerOptions options) => reader.GetSingle();
        public override void Write(Utf8JsonWriter writer, float value, JsonSerializerOptions options) => WriteFloating(writer, value);
    }
    private sealed class BigIntegerConverter : JsonConverter<BigInteger>
    {
        public override BigInteger Read(ref Utf8JsonReader reader, Type type, JsonSerializerOptions options)
        {
            using var document = JsonDocument.ParseValue(ref reader);
            return BigInteger.Parse(document.RootElement.GetRawText(), CultureInfo.InvariantCulture);
        }
        public override void Write(Utf8JsonWriter writer, BigInteger value, JsonSerializerOptions options) =>
            writer.WriteRawValue(value.ToString(CultureInfo.InvariantCulture));
    }
}
