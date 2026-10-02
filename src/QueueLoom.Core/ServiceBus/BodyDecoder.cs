using System.Buffers.Binary;
using System.IO.Compression;
using System.Text;
using System.Text.Json;

namespace QueueLoom.Core.ServiceBus;

/// <summary>A message body after unpacking, with the steps that were applied ("base64", "gzip", "Avro"…).</summary>
public sealed record DecodedBody(IReadOnlyList<string> Steps, string Text, bool IsJson, string? Note = null)
{
    public string Summary => string.Join(" → ", Steps);
}

/// <summary>
/// Unpacks bodies that are not readable as they are: gzip and zlib compression, base64 text, Avro object container
/// files (with the schema they carry) and Protobuf messages (field numbers only, since no .proto file is known).
/// Returns null for plain text and JSON, which the body view already shows.
/// </summary>
public static class BodyDecoder
{
    public const int MaximumDecodedBytes = 16 * 1024 * 1024;
    private const int MaximumSteps = 5;
    private const int MaximumRecords = 1_000;
    private static readonly UTF8Encoding StrictUtf8 = new(false, throwOnInvalidBytes: true);
    private static readonly JsonWriterOptions Indented = new()
    {
        Indented = true,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    /// <summary>
    /// The registry schema id of a body in the Confluent wire format: a zero magic byte, then the id as a 4-byte
    /// big-endian integer, then the Avro, Protobuf or JSON payload.
    /// </summary>
    public static bool TryReadSchemaId(ReadOnlySpan<byte> body, out int schemaId)
    {
        schemaId = 0;
        if (body.Length < 5 || body[0] != 0)
        {
            return false;
        }
        schemaId = BinaryPrimitives.ReadInt32BigEndian(body[1..5]);
        return schemaId > 0;
    }

    /// <param name="protos">Message types for Protobuf bodies; <see cref="ProtoSchemaCatalog.Current"/> when null.</param>
    /// <param name="messageType">The message type the message names, if any (see <see cref="ProtoSchemaCatalog.HintFrom"/>).</param>
    public static DecodedBody? Decode(ReadOnlyMemory<byte> body, string? contentType = null, MessageSchema? schema = null,
        ProtoSchemaSet? protos = null, string? messageType = null)
    {
        protos ??= ProtoSchemaCatalog.Current;
        if (TryReadSchemaId(body.Span, out var schemaId))
        {
            var registry = DecodeRegistryFramed(body[5..], schemaId, schema?.Id == schemaId ? schema : null);
            if (registry is not null)
            {
                return registry;
            }
        }

        var steps = new List<string>();
        string? note = null;
        var current = body;
        for (var round = 0; round < MaximumSteps; round++)
        {
            var span = current.Span;
            if (IsGzip(span) || IsZlib(span))
            {
                var gzip = IsGzip(span);
                var (bytes, truncated) = Decompress(current, gzip);
                if (bytes is null)
                {
                    break;
                }
                steps.Add(gzip ? "gzip" : "zlib");
                note ??= truncated ? $"Only the first {MaximumDecodedBytes / (1024 * 1024)} MiB are shown." : null;
                current = bytes;
                continue;
            }

            if (IsAvroContainer(span))
            {
                var (json, avroNote) = AvroContainer.ToJson(span);
                if (json is not null)
                {
                    steps.Add("Avro");
                    return new DecodedBody(steps, json, true, avroNote ?? note);
                }
                return new DecodedBody([.. steps, "Avro"], HexDump(span), false, avroNote);
            }

            if (TryBase64(span, out var decoded) && IsWorthDecoding(decoded))
            {
                steps.Add("base64");
                current = decoded;
                continue;
            }

            break;
        }

        var final = current.Span;
        if (TryText(final, out var text))
        {
            if (TryIndentJson(text, out var json))
            {
                steps.Add("JSON");
                return steps.Count > 1 ? new DecodedBody(steps, json, true, note) : null;
            }
            return steps.Count > 0 ? new DecodedBody([.. steps, "text"], text, false, note) : null;
        }

        var protobufHint = contentType?.Contains("proto", StringComparison.OrdinalIgnoreCase) == true;
        messageType ??= ProtoSchemaCatalog.HintFrom(contentType);
        if (!protos.IsEmpty && ProtoDecoder.DecodeBestFit(final, protos, messageType) is { } typed)
        {
            steps.Add($"Protobuf ({typed.Type.FullName})");
            var named = protos.Resolve(messageType) == typed.Type;
            return new DecodedBody(steps, typed.Json, true, note ??
                (named ? $"Message type {typed.Type.FullName}, as the message says." : $"Message type {typed.Type.FullName}: the loaded type that fits the body."));
        }
        if (Protobuf.TryToJson(final, requireMessage: !protobufHint, out var protobuf))
        {
            steps.Add("Protobuf (no schema)");
            return new DecodedBody(steps, protobuf, true,
                note ?? "Without the .proto file, fields are shown by number and nested messages are guessed.");
        }

        return steps.Count > 0 ? new DecodedBody([.. steps, "binary"], HexDump(final), false, note) : null;
    }

    /// <summary>
    /// A body framed for a Schema Registry. With the schema, Avro is read field by field and Protobuf skips its
    /// message indexes; without it only JSON payloads are recognised, so random binary starting with a zero byte
    /// is not mistaken for a framed message.
    /// </summary>
    private static DecodedBody? DecodeRegistryFramed(ReadOnlyMemory<byte> payload, int schemaId, MessageSchema? schema)
    {
        var span = payload.Span;
        if (schema is null)
        {
            return TryText(span, out var plain) && TryIndentJson(plain, out var framedJson)
                ? new DecodedBody(["Schema Registry", "JSON"], framedJson, true,
                    $"Schema id {schemaId}. Set the Schema Registry URL of this environment to see the schema.")
                : null;
        }

        switch (schema.Type)
        {
            case MessageSchemaType.Json:
                return TryText(span, out var text) && TryIndentJson(text, out var json)
                    ? new DecodedBody(["Schema Registry", "JSON"], json, true, $"Schema id {schemaId} (JSON Schema).")
                    : null;
            case MessageSchemaType.Protobuf:
                var position = 0;
                if (!Protobuf.TryReadVarint(span, ref position, out var rawCount))
                {
                    return null;
                }
                var count = (long)(rawCount >> 1) ^ -(long)(rawCount & 1);
                for (var index = 0L; index < count; index++)
                {
                    if (!Protobuf.TryReadVarint(span, ref position, out _))
                    {
                        return null;
                    }
                }
                var indexes = new List<int>();
                var indexPosition = 0;
                Protobuf.TryReadVarint(span, ref indexPosition, out var rawIndexCount);
                for (var index = 0L; index < ((long)(rawIndexCount >> 1) ^ -(long)(rawIndexCount & 1)); index++)
                {
                    Protobuf.TryReadVarint(span, ref indexPosition, out var rawIndex);
                    indexes.Add((int)((long)(rawIndex >> 1) ^ -(long)(rawIndex & 1)));
                }
                if (RegistryProtoType(schema, indexes) is { } registry &&
                    ProtoDecoder.Decode(span[position..], registry.Type, registry.Schemas) is { } typedRegistry)
                {
                    return new DecodedBody(["Schema Registry", "Protobuf"], typedRegistry.Json, true,
                        $"Schema id {schemaId}, message {registry.Type.FullName}.");
                }
                var message = ProtobufMessageName(schema.Text);
                return Protobuf.TryToJson(span[position..], requireMessage: false, out var protobuf)
                    ? new DecodedBody(["Schema Registry", "Protobuf"], protobuf, true,
                        $"Schema id {schemaId}{(message is null ? string.Empty : $", message {message}")}. Fields are shown by number, as in the .proto file.")
                    : null;
            default:
                try
                {
                    using var document = JsonDocument.Parse(schema.Text);
                    var avro = new AvroSchema(document.RootElement);
                    if (span.Length > MaximumDecodedBytes) throw new InvalidDataException("Avro decoding exceeds the 16 MiB limit.");
                    var reader = new AvroReader(span.ToArray(), 0);
                    using var stream = new AvroContainer.BoundedAvroOutput();
                    using (var writer = new Utf8JsonWriter(stream, Indented))
                    {
                        avro.Write(writer, avro.Root, reader);
                    }
                    var name = document.RootElement.ValueKind == JsonValueKind.Object &&
                               document.RootElement.TryGetProperty("name", out var named)
                        ? named.GetString()
                        : null;
                    var note = $"Schema id {schemaId}{(name is null ? string.Empty : $", record {name}")}." +
                               (reader.AtEnd ? string.Empty : " The body is longer than the schema describes; it may have been written with another schema.");
                    return new DecodedBody(["Schema Registry", "Avro"], Encoding.UTF8.GetString(stream.ToArray()), true, note);
                }
                catch (Exception exception) when (exception is FormatException or JsonException or InvalidDataException
                                                      or ArgumentException or OverflowException or IndexOutOfRangeException
                                                      or InvalidOperationException or KeyNotFoundException)
                {
                    return new DecodedBody(["Schema Registry", "Avro"], HexDump(span), false,
                        $"Schema id {schemaId}: the body does not match the schema ({exception.Message}).");
                }
        }
    }

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, ProtoSchemaSet?> RegistryProtos = new(StringComparer.Ordinal);

    /// <summary>
    /// The message type a Confluent-framed body names: its message indexes walk the .proto's top-level messages and
    /// then their nested ones ([] means the first message). Null when the schema cannot be read, for example because
    /// it imports types the registry keeps elsewhere.
    /// </summary>
    private static (ProtoMessageType Type, ProtoSchemaSet Schemas)? RegistryProtoType(MessageSchema schema, IReadOnlyList<int> indexes)
    {
        var schemas = RegistryProtos.GetOrAdd(schema.Text, text =>
        {
            try
            {
                return ProtoSchemaSet.FromProtoFiles([("registry.proto", text)]);
            }
            catch (ProtoSchemaException)
            {
                return null;
            }
        });
        if (schemas is null || schemas.IsEmpty)
        {
            return null;
        }
        var all = schemas.AllMessages;
        var names = all.Select(message => message.FullName).ToHashSet(StringComparer.Ordinal);
        string Parent(string name) => name.LastIndexOf('.') is var dot and > 0 ? name[..dot] : string.Empty;
        var level = all.Where(message => !names.Contains(Parent(message.FullName))).ToArray();
        ProtoMessageType? current = null;
        foreach (var index in indexes.Count == 0 ? [0] : indexes)
        {
            if (index < 0 || index >= level.Length)
            {
                return null;
            }
            current = level[index];
            var parent = current.FullName;
            level = all.Where(message => Parent(message.FullName) == parent).ToArray();
        }
        return current is null ? null : (current, schemas);
    }

    /// <summary>The first message declared in a .proto file, which Confluent serializers use when no index is given.</summary>
    private static string? ProtobufMessageName(string proto)
    {
        var match = System.Text.RegularExpressions.Regex.Match(proto, @"\bmessage\s+([A-Za-z_][A-Za-z0-9_]*)");
        return match.Success ? match.Groups[1].Value : null;
    }

    private static bool IsGzip(ReadOnlySpan<byte> span) => span.Length > 18 && span[0] == 0x1F && span[1] == 0x8B && span[2] == 8;

    private static bool IsZlib(ReadOnlySpan<byte> span) =>
        span.Length > 6 && (span[0] & 0x0F) == 8 && (span[0] >> 4) <= 7 && (span[0] * 256 + span[1]) % 31 == 0 && (span[1] & 0x20) == 0;

    private static bool IsAvroContainer(ReadOnlySpan<byte> span) =>
        span.Length > 20 && span[0] == (byte)'O' && span[1] == (byte)'b' && span[2] == (byte)'j' && span[3] == 1;

    /// <summary>Base64 is only worth unpacking when it hides something: compressed data, Avro, JSON or readable text.</summary>
    private static bool IsWorthDecoding(ReadOnlyMemory<byte> decoded)
    {
        var span = decoded.Span;
        return IsGzip(span) || IsZlib(span) || IsAvroContainer(span) ||
               TryText(span, out var text) && text.Length >= 4 && text.Count(char.IsLetterOrDigit) * 2 >= text.Length;
    }

    private static (byte[]? Bytes, bool Truncated) Decompress(ReadOnlyMemory<byte> data, bool gzip)
    {
        try
        {
            using var input = new MemoryStream(data.ToArray(), writable: false);
            using Stream stream = gzip
                ? new GZipStream(input, CompressionMode.Decompress)
                : new ZLibStream(input, CompressionMode.Decompress);
            using var output = new MemoryStream();
            var buffer = new byte[81_920];
            int read;
            while ((read = stream.Read(buffer, 0, buffer.Length)) > 0)
            {
                var room = MaximumDecodedBytes - (int)output.Length;
                output.Write(buffer, 0, Math.Min(read, room));
                if (read > room)
                {
                    return (output.ToArray(), true);
                }
            }
            return output.Length == 0 ? (null, false) : (output.ToArray(), false);
        }
        catch (InvalidDataException)
        {
            return (null, false);
        }
    }

    private static bool TryBase64(ReadOnlySpan<byte> span, out ReadOnlyMemory<byte> decoded)
    {
        decoded = default;
        if (!TryText(span, out var text))
        {
            return false;
        }

        // Line breaks are allowed (MIME base64); any other space means this is ordinary text.
        var compact = text.Trim().Replace("\r", string.Empty, StringComparison.Ordinal).Replace("\n", string.Empty, StringComparison.Ordinal);
        if (compact.Length < 12 || compact.Length % 4 == 1 ||
            !compact.All(character => char.IsAsciiLetterOrDigit(character) || character is '+' or '/' or '-' or '_' or '='))
        {
            return false;
        }

        var standard = compact.Replace('-', '+').Replace('_', '/');
        standard = standard.PadRight(standard.Length + (4 - standard.Length % 4) % 4, '=');
        try
        {
            decoded = Convert.FromBase64String(standard);
            return decoded.Length > 0;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    private static bool TryText(ReadOnlySpan<byte> span, out string text)
    {
        text = string.Empty;
        try
        {
            text = StrictUtf8.GetString(span);
        }
        catch (DecoderFallbackException)
        {
            return false;
        }
        return !text.Any(character => char.IsControl(character) && character is not ('\r' or '\n' or '\t'));
    }

    private static bool TryIndentJson(string text, out string json)
    {
        json = text;
        var trimmed = text.TrimStart();
        if (trimmed.Length == 0 || trimmed[0] is not ('{' or '['))
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(text);
            using var stream = new MemoryStream();
            using (var writer = new Utf8JsonWriter(stream, Indented))
            {
                document.RootElement.WriteTo(writer);
            }
            json = Encoding.UTF8.GetString(stream.ToArray());
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    /// <summary>Offset, 16 bytes in hex and their printable characters per line; the first 64 KiB.</summary>
    public static string HexDump(ReadOnlySpan<byte> span)
    {
        const int Limit = 64 * 1024;
        var builder = new StringBuilder();
        var length = Math.Min(span.Length, Limit);
        for (var offset = 0; offset < length; offset += 16)
        {
            var line = span.Slice(offset, Math.Min(16, length - offset));
            builder.Append(offset.ToString("X8", System.Globalization.CultureInfo.InvariantCulture)).Append("  ");
            for (var index = 0; index < 16; index++)
            {
                builder.Append(index < line.Length ? line[index].ToString("X2", System.Globalization.CultureInfo.InvariantCulture) + " " : "   ");
                if (index == 7)
                {
                    builder.Append(' ');
                }
            }
            builder.Append(' ');
            foreach (var value in line)
            {
                builder.Append(value is >= 0x20 and < 0x7F ? (char)value : '.');
            }
            builder.Append('\n');
        }
        if (span.Length > Limit)
        {
            builder.Append($"… {span.Length - Limit:N0} more bytes");
        }
        return builder.ToString();
    }

    /// <summary>Schema-less Protobuf: field numbers with their values; length-delimited fields become text, nested messages or base64.</summary>
    internal static class Protobuf
    {
        private const int MaximumDepth = 12;

        public static bool TryToJson(ReadOnlySpan<byte> data, bool requireMessage, out string json)
        {
            json = string.Empty;
            if (data.Length == 0 || !IsMessage(data, 0))
            {
                return false;
            }
            if (requireMessage && data.Length < 2)
            {
                return false;
            }

            using var stream = new MemoryStream();
            using (var writer = new Utf8JsonWriter(stream, Indented))
            {
                WriteMessage(writer, data, 0);
            }
            json = Encoding.UTF8.GetString(stream.ToArray());
            return true;
        }

        private static bool IsMessage(ReadOnlySpan<byte> data, int depth)
        {
            if (depth > MaximumDepth || data.Length == 0)
            {
                return false;
            }

            var position = 0;
            while (position < data.Length)
            {
                if (!TryReadVarint(data, ref position, out var tag))
                {
                    return false;
                }
                var field = tag >> 3;
                if (field is 0 or > 536_870_911)
                {
                    return false;
                }
                switch (tag & 7)
                {
                    case 0:
                        if (!TryReadVarint(data, ref position, out _))
                        {
                            return false;
                        }
                        break;
                    case 1:
                        position += 8;
                        break;
                    case 2:
                        if (!TryReadVarint(data, ref position, out var length) || length > (ulong)(data.Length - position))
                        {
                            return false;
                        }
                        position += (int)length;
                        break;
                    case 5:
                        position += 4;
                        break;
                    default:
                        return false;
                }
                if (position > data.Length)
                {
                    return false;
                }
            }
            return true;
        }

        private static void WriteMessage(Utf8JsonWriter writer, ReadOnlySpan<byte> data, int depth)
        {
            var fields = new List<(ulong Field, Action<Utf8JsonWriter> Write)>();
            var position = 0;
            while (position < data.Length)
            {
                TryReadVarint(data, ref position, out var tag);
                var field = tag >> 3;
                switch (tag & 7)
                {
                    case 0:
                        TryReadVarint(data, ref position, out var varint);
                        fields.Add((field, w => w.WriteNumberValue(varint)));
                        break;
                    case 1:
                        var fixed64 = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(position, 8));
                        position += 8;
                        fields.Add((field, w => w.WriteNumberValue(fixed64)));
                        break;
                    case 5:
                        var fixed32 = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(position, 4));
                        position += 4;
                        fields.Add((field, w => w.WriteNumberValue(fixed32)));
                        break;
                    default:
                        TryReadVarint(data, ref position, out var length);
                        var bytes = data.Slice(position, (int)length).ToArray();
                        position += (int)length;
                        fields.Add((field, w => WriteLengthDelimited(w, bytes, depth)));
                        break;
                }
            }

            writer.WriteStartObject();
            foreach (var group in fields.GroupBy(item => item.Field))
            {
                writer.WritePropertyName(group.Key.ToString(System.Globalization.CultureInfo.InvariantCulture));
                var values = group.ToArray();
                if (values.Length > 1)
                {
                    writer.WriteStartArray();
                }
                foreach (var value in values)
                {
                    value.Write(writer);
                }
                if (values.Length > 1)
                {
                    writer.WriteEndArray();
                }
            }
            writer.WriteEndObject();
        }

        private static void WriteLengthDelimited(Utf8JsonWriter writer, byte[] bytes, int depth)
        {
            if (bytes.Length > 0 && TryText(bytes, out var text) && text.Count(character => !char.IsControl(character)) == text.Length)
            {
                writer.WriteStringValue(text);
            }
            else if (IsMessage(bytes, depth + 1))
            {
                WriteMessage(writer, bytes, depth + 1);
            }
            else
            {
                writer.WriteStringValue(bytes.Length == 0 ? string.Empty : "base64:" + Convert.ToBase64String(bytes));
            }
        }

        internal static bool TryReadVarint(ReadOnlySpan<byte> data, ref int position, out ulong value)
        {
            value = 0;
            for (var shift = 0; shift < 64; shift += 7)
            {
                if (position >= data.Length)
                {
                    return false;
                }
                var next = data[position++];
                value |= (ulong)(next & 0x7F) << shift;
                if ((next & 0x80) == 0)
                {
                    return true;
                }
            }
            return false;
        }
    }

    /// <summary>Avro object container files: the writer's schema from the header, then every record as JSON.</summary>
    internal static class AvroContainer
    {
        private const string LimitMessage = "Avro decoding exceeds the 16 MiB limit.";
        public static (string? Json, string? Note) ToJson(ReadOnlySpan<byte> data)
        {
            try
            {
                if (data.Length > MaximumDecodedBytes) throw new InvalidDataException(LimitMessage);
                var reader = new AvroReader(data.ToArray(), 4);
                var metadata = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                for (var count = reader.ReadLong(); count != 0; count = reader.ReadLong())
                {
                    if (count < 0)
                    {
                        count = -count;
                        reader.ReadLong();
                    }
                    for (var index = 0; index < count; index++)
                    {
                        metadata[reader.ReadString()] = reader.ReadBytes();
                    }
                }
                var sync = reader.ReadFixed(16);
                if (!metadata.TryGetValue("avro.schema", out var schemaBytes))
                {
                    return (null, "The Avro file has no schema.");
                }

                var codec = metadata.TryGetValue("avro.codec", out var codecBytes) ? Encoding.UTF8.GetString(codecBytes) : "null";
                if (codec is not ("null" or "deflate"))
                {
                    return (null, $"Avro data compressed with '{codec}' cannot be read here; only uncompressed and deflate are supported.");
                }

                using var schemaDocument = JsonDocument.Parse(schemaBytes);
                var schema = new AvroSchema(schemaDocument.RootElement);
                using var stream = new BoundedAvroOutput();
                var remainingBytes = MaximumDecodedBytes;
                var records = 0;
                var truncated = false;
                using (var writer = new Utf8JsonWriter(stream, Indented))
                {
                    writer.WriteStartArray();
                    while (!reader.AtEnd && !truncated)
                    {
                        var count = reader.ReadLong();
                        var size = reader.ReadLong();
                        var block = reader.ReadFixed(checked((int)size));
                        if (codec == "deflate")
                        {
                            using var input = new DeflateStream(new MemoryStream(block), CompressionMode.Decompress);
                            using var output = new MemoryStream();
                            var buffer = new byte[64 * 1024];
                            int read;
                            while ((read = input.Read(buffer, 0, Math.Min(buffer.Length, remainingBytes + 1))) > 0)
                            {
                                if (read > remainingBytes) throw new InvalidDataException(LimitMessage);
                                output.Write(buffer, 0, read);
                                remainingBytes -= read;
                            }
                            block = output.ToArray();
                        }
                        else
                        {
                            if (block.Length > remainingBytes) throw new InvalidDataException(LimitMessage);
                            remainingBytes -= block.Length;
                        }

                        var blockReader = new AvroReader(block, 0);
                        for (var index = 0; index < count; index++)
                        {
                            if (records++ >= MaximumRecords)
                            {
                                truncated = true;
                                break;
                            }
                            schema.Write(writer, schema.Root, blockReader);
                        }
                        if (!reader.ReadFixed(16).AsSpan().SequenceEqual(sync))
                        {
                            return (null, "The Avro file is damaged: a block does not end with the file's sync marker.");
                        }
                    }
                    writer.WriteEndArray();
                }

                var name = schemaDocument.RootElement.ValueKind == JsonValueKind.Object &&
                           schemaDocument.RootElement.TryGetProperty("name", out var named)
                    ? named.GetString()
                    : null;
                var note = $"{Math.Min(records, MaximumRecords):N0} record(s)" + (name is null ? string.Empty : $" of {name}") +
                           (truncated ? $"; only the first {MaximumRecords:N0} are shown" : string.Empty) + ".";
                return (Encoding.UTF8.GetString(stream.ToArray()), note);
            }
            catch (Exception exception) when (exception is FormatException or JsonException or InvalidDataException
                                                  or ArgumentException or OverflowException or IndexOutOfRangeException
                                                  or InvalidOperationException)
            {
                return (null, $"The Avro data could not be read: {exception.Message}");
            }
        }

        // Limit JSON expansion as it is written, including escape sequences and large collections.
        internal sealed class BoundedAvroOutput : MemoryStream
        {
            public override void Write(ReadOnlySpan<byte> buffer)
            {
                if (Length + buffer.Length > MaximumDecodedBytes) throw new InvalidDataException(LimitMessage);
                var bytes = buffer.ToArray();
                base.Write(bytes, 0, bytes.Length);
            }
            public override void Write(byte[] buffer, int offset, int count)
            {
                if (Length + count > MaximumDecodedBytes) throw new InvalidDataException(LimitMessage);
                base.Write(buffer, offset, count);
            }
        }
    }

    private sealed class AvroReader(byte[] data, int position)
    {
        private int _position = position;

        public bool AtEnd => _position >= data.Length;

        public long ReadLong()
        {
            ulong value = 0;
            for (var shift = 0; shift < 64; shift += 7)
            {
                var next = data[_position++];
                value |= (ulong)(next & 0x7F) << shift;
                if ((next & 0x80) == 0)
                {
                    return (long)(value >> 1) ^ -(long)(value & 1);
                }
            }
            throw new FormatException("A number in the Avro data is too long.");
        }

        public byte[] ReadBytes() => ReadFixed(checked((int)ReadLong()));

        public string ReadString() => Encoding.UTF8.GetString(ReadBytes());

        public byte[] ReadFixed(int length)
        {
            if (length < 0 || _position + length > data.Length)
            {
                throw new FormatException("The Avro data ends too early.");
            }
            var bytes = data.AsSpan(_position, length).ToArray();
            _position += length;
            return bytes;
        }

        public float ReadFloat() => BinaryPrimitives.ReadSingleLittleEndian(ReadFixed(4));

        public double ReadDouble() => BinaryPrimitives.ReadDoubleLittleEndian(ReadFixed(8));

        public bool ReadBoolean() => ReadFixed(1)[0] != 0;
    }

    /// <summary>Walks an Avro schema (JSON) and writes the matching binary data as JSON values.</summary>
    private sealed class AvroSchema
    {
        private readonly Dictionary<string, JsonElement> _named = new(StringComparer.Ordinal);

        public AvroSchema(JsonElement root)
        {
            Root = root;
            Register(root, null);
        }

        public JsonElement Root { get; }

        public void Write(Utf8JsonWriter writer, JsonElement schema, AvroReader reader)
        {
            switch (schema.ValueKind)
            {
                case JsonValueKind.String:
                    WriteNamedOrPrimitive(writer, schema.GetString()!, reader);
                    return;
                case JsonValueKind.Array:
                    var branch = checked((int)reader.ReadLong());
                    Write(writer, schema[branch], reader);
                    return;
                case JsonValueKind.Object:
                    break;
                default:
                    throw new FormatException("The Avro schema is not valid.");
            }

            var type = schema.GetProperty("type");
            if (type.ValueKind != JsonValueKind.String)
            {
                Write(writer, type, reader);
                return;
            }

            switch (type.GetString())
            {
                case "record" or "error":
                    writer.WriteStartObject();
                    foreach (var field in schema.GetProperty("fields").EnumerateArray())
                    {
                        writer.WritePropertyName(EncodeBoundedText(writer, field.GetProperty("name").GetString()!));
                        Write(writer, field.GetProperty("type"), reader);
                    }
                    writer.WriteEndObject();
                    return;
                case "enum":
                    writer.WriteStringValue(EncodeBoundedText(writer, schema.GetProperty("symbols")[checked((int)reader.ReadLong())].GetString()!));
                    return;
                case "array":
                    writer.WriteStartArray();
                    ReadBlocks(reader, () => Write(writer, schema.GetProperty("items"), reader));
                    writer.WriteEndArray();
                    return;
                case "map":
                    writer.WriteStartObject();
                    ReadBlocks(reader, () =>
                    {
                        writer.WritePropertyName(EncodeBoundedText(writer, reader.ReadString()));
                        Write(writer, schema.GetProperty("values"), reader);
                    });
                    writer.WriteEndObject();
                    return;
                case "fixed":
                    var fixedSize = schema.GetProperty("size").GetInt32();
                    if (fixedSize > RemainingJsonBytes(writer) / 2) throw new InvalidDataException("Avro decoding exceeds the 16 MiB limit.");
                    writer.WriteStringValue(EncodeBoundedText(writer, Convert.ToHexString(reader.ReadFixed(fixedSize))));
                    return;
                default:
                    WriteNamedOrPrimitive(writer, type.GetString()!, reader);
                    return;
            }
        }

        private void WriteNamedOrPrimitive(Utf8JsonWriter writer, string type, AvroReader reader)
        {
            switch (type)
            {
                case "null":
                    writer.WriteNullValue();
                    return;
                case "boolean":
                    writer.WriteBooleanValue(reader.ReadBoolean());
                    return;
                case "int" or "long":
                    writer.WriteNumberValue(reader.ReadLong());
                    return;
                case "float":
                    WriteFloatingPoint(writer, reader.ReadFloat());
                    return;
                case "double":
                    WriteFloatingPoint(writer, reader.ReadDouble());
                    return;
                case "bytes":
                    var bytes = reader.ReadBytes();
                    if ((bytes.LongLength + 2) / 3 * 4 > RemainingJsonBytes(writer)) throw new InvalidDataException("Avro decoding exceeds the 16 MiB limit.");
                    writer.WriteStringValue(EncodeBoundedText(writer, Convert.ToBase64String(bytes)));
                    return;
                case "string":
                    writer.WriteStringValue(EncodeBoundedText(writer, reader.ReadString()));
                    return;
            }

            if (!_named.TryGetValue(type, out var named))
            {
                throw new FormatException($"The Avro schema refers to an unknown type '{type}'.");
            }
            Write(writer, named, reader);
        }

        private static long RemainingJsonBytes(Utf8JsonWriter writer) => MaximumDecodedBytes - writer.BytesCommitted - writer.BytesPending - 2;

        private static JsonEncodedText EncodeBoundedText(Utf8JsonWriter writer, string text)
        {
            // Check expansion before allocating escaped text or asking Utf8JsonWriter for a large token buffer.
            var encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping;
            long bytes = 0;
            var remaining = RemainingJsonBytes(writer);
            foreach (var rune in text.EnumerateRunes())
            {
                bytes += rune.Value is '"' or '\\' or '\b' or '\f' or '\n' or '\r' or '\t' ? 2 :
                    encoder.WillEncode(rune.Value) ? rune.Value > 0xffff ? 12 : 6 : rune.Utf8SequenceLength;
                if (bytes > remaining) throw new InvalidDataException("Avro decoding exceeds the 16 MiB limit.");
            }
            var encoded = JsonEncodedText.Encode(text, encoder);
            if (encoded.EncodedUtf8Bytes.Length > remaining) throw new InvalidDataException("Avro decoding exceeds the 16 MiB limit.");
            return encoded;
        }

        private static void WriteFloatingPoint(Utf8JsonWriter writer, double value)
        {
            if (double.IsFinite(value))
            {
                writer.WriteNumberValue(value);
            }
            else
            {
                writer.WriteStringValue(value.ToString(System.Globalization.CultureInfo.InvariantCulture));
            }
        }

        private static void ReadBlocks(AvroReader reader, Action readItem)
        {
            for (var count = reader.ReadLong(); count != 0; count = reader.ReadLong())
            {
                if (count < 0)
                {
                    count = -count;
                    reader.ReadLong();
                }
                for (var index = 0; index < count; index++)
                {
                    readItem();
                }
            }
        }

        private void Register(JsonElement schema, string? enclosingNamespace)
        {
            switch (schema.ValueKind)
            {
                case JsonValueKind.Array:
                    foreach (var branch in schema.EnumerateArray())
                    {
                        Register(branch, enclosingNamespace);
                    }
                    return;
                case JsonValueKind.Object:
                    break;
                default:
                    return;
            }

            var space = schema.TryGetProperty("namespace", out var ns) ? ns.GetString() : enclosingNamespace;
            if (schema.TryGetProperty("name", out var name) && name.GetString() is { } shortName)
            {
                var fullName = shortName.Contains('.', StringComparison.Ordinal) || string.IsNullOrEmpty(space)
                    ? shortName
                    : $"{space}.{shortName}";
                _named[fullName] = schema;
                _named.TryAdd(shortName, schema);
                _named.TryAdd(fullName[(fullName.LastIndexOf('.') + 1)..], schema);
            }

            if (schema.TryGetProperty("fields", out var fields))
            {
                foreach (var field in fields.EnumerateArray())
                {
                    Register(field.GetProperty("type"), space);
                }
            }
            foreach (var nested in new[] { "items", "values", "type" })
            {
                if (schema.TryGetProperty(nested, out var inner) && inner.ValueKind != JsonValueKind.String)
                {
                    Register(inner, space);
                }
            }
        }
    }
}
