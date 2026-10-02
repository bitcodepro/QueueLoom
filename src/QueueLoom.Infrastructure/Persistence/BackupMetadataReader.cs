using System.Text;
using System.Text.Json;

namespace QueueLoom.Infrastructure.Persistence;

/// <summary>Projects root metadata with bounded memory, including legacy files whose body occurs first.
/// Large strings and nested values are scanned without collecting them. Full JSON/body validation is lazy.</summary>
internal static class BackupMetadataReader
{
    private static readonly HashSet<string> Fields = new(StringComparer.Ordinal)
    {
        "schemaVersion", "profileId", "profileName", "environment", "fullyQualifiedNamespace", "sourceKind",
        "sourcePath", "sourceName", "topicName", "subQueue", "sequenceNumber", "messageId", "correlationId",
        "subject", "enqueuedTimeUtc", "backedUpAtUtc", "bodySize"
    };

    public static JsonDocument Read(string path, CancellationToken token)
    {
        using var stream = File.OpenRead(path);
        if (stream.Length > 512L * 1024 * 1024) throw new InvalidDataException("Backup exceeds the 512 MiB metadata scan limit.");
        using var reader = new StreamReader(stream, Encoding.UTF8, true, 64 * 1024);
        var input = new Input(reader, token);
        var output = new StringBuilder("{");
        if (input.NextNonSpace() != '{') throw new InvalidDataException("Backup must be a JSON object.");
        var first = true;
        while (true)
        {
            var start = input.NextNonSpace();
            if (start == '}') break;
            if (start != '"') throw new InvalidDataException("Invalid backup metadata key.");
            var key = new StringBuilder("\"");
            ReadString(input, key);
            var name = JsonSerializer.Deserialize<string>(key.ToString());
            if (input.NextNonSpace() != ':') throw new InvalidDataException("Invalid backup metadata separator.");
            var keep = name is not null && Fields.Contains(name);
            var value = keep ? new StringBuilder() : null;
            ReadValue(input, value);
            if (keep)
            {
                if (!first) output.Append(',');
                output.Append(key).Append(':').Append(value);
                first = false;
                if (output.Length > 256 * 1024) throw new InvalidDataException("Backup metadata exceeds 256 KiB.");
            }
            var delimiter = input.NextNonSpace();
            if (delimiter == '}') break;
            if (delimiter != ',') throw new InvalidDataException("Invalid backup value delimiter.");
        }
        if (input.NextNonSpace() != -1) throw new InvalidDataException("Trailing backup content.");
        output.Append('}');
        return JsonDocument.Parse(output.ToString());
    }

    private static void Append(StringBuilder? output, int ch)
    {
        if (output is null) return;
        if (output.Length >= 64 * 1024) throw new InvalidDataException("Backup metadata value exceeds 64 KiB.");
        output.Append((char)ch);
    }

    private static void ReadString(Input input, StringBuilder? output)
    {
        while (true)
        {
            var ch = input.Read();
            if (ch < 0 || ch < 32) throw new InvalidDataException("Unterminated or invalid backup string.");
            Append(output, ch);
            if (ch == '"') return;
            if (ch == '\\')
            {
                ch = input.Read();
                if (ch < 0) throw new InvalidDataException("Unterminated backup escape.");
                Append(output, ch);
            }
        }
    }

    private static void ReadValue(Input input, StringBuilder? output)
    {
        var ch = input.NextNonSpace();
        if (ch < 0) throw new InvalidDataException("Missing backup value.");
        Append(output, ch);
        if (ch == '"') { ReadString(input, output); return; }
        if (ch is '{' or '[')
        {
            var depth = 1;
            while (depth > 0)
            {
                ch = input.Read();
                if (ch < 0) throw new InvalidDataException("Unterminated backup value.");
                Append(output, ch);
                if (ch == '"') ReadString(input, output);
                else if (ch is '{' or '[') { if (++depth > 64) throw new InvalidDataException("Backup nesting exceeds 64 levels."); }
                else if (ch is '}' or ']') depth--;
            }
            return;
        }
        while ((ch = input.Peek()) >= 0 && ch is not ',' and not '}' && !char.IsWhiteSpace((char)ch)) Append(output, input.Read());
    }

    private sealed class Input(StreamReader reader, CancellationToken token)
    {
        private readonly char[] _buffer = new char[64 * 1024];
        private int _offset;
        private int _count;
        public int Peek()
        {
            if (_offset < _count) return _buffer[_offset];
            token.ThrowIfCancellationRequested();
            _count = reader.Read(_buffer, 0, _buffer.Length);
            _offset = 0;
            return _count == 0 ? -1 : _buffer[0];
        }
        public int Read() { var ch = Peek(); if (ch >= 0) _offset++; return ch; }
        public int NextNonSpace() { int ch; do { ch = Read(); } while (ch >= 0 && char.IsWhiteSpace((char)ch)); return ch; }
    }
}
