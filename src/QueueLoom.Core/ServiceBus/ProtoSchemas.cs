using System.Text;

namespace QueueLoom.Core.ServiceBus;

/// <summary>Field types, numbered as in google/protobuf/descriptor.proto.</summary>
public enum ProtoFieldType
{
    Double = 1,
    Float = 2,
    Int64 = 3,
    UInt64 = 4,
    Int32 = 5,
    Fixed64 = 6,
    Fixed32 = 7,
    Bool = 8,
    String = 9,
    Group = 10,
    Message = 11,
    Bytes = 12,
    UInt32 = 13,
    Enum = 14,
    SFixed32 = 15,
    SFixed64 = 16,
    SInt32 = 17,
    SInt64 = 18
}

/// <summary>A field of a message; <see cref="TypeName"/> is the full name of its message or enum type.</summary>
public sealed record ProtoField(string Name, int Number, ProtoFieldType Type, string? TypeName, bool IsRepeated)
{
    /// <summary>The containing oneof, scoped to this message; null for an ordinary field.</summary>
    public string? Oneof { get; init; }
}

public sealed class ProtoMessageType(string fullName, IReadOnlyList<ProtoField> fields, bool isMapEntry = false)
{
    public string FullName { get; } = fullName;

    public string Name => FullName[(FullName.LastIndexOf('.') + 1)..];

    public IReadOnlyList<ProtoField> Fields { get; } = fields;

    public IReadOnlyDictionary<int, ProtoField> FieldsByNumber { get; } = fields.GroupBy(field => field.Number).ToDictionary(group => group.Key, group => group.First());

    /// <summary>The generated entry type of a map field: key is field 1, value field 2.</summary>
    public bool IsMapEntry { get; } = isMapEntry;
}

public sealed class ProtoEnumType(string fullName, IReadOnlyDictionary<int, string> values)
{
    public string FullName { get; } = fullName;

    public IReadOnlyDictionary<int, string> Values { get; } = values;
}

/// <summary>A .proto file or descriptor set that cannot be read, with the reason.</summary>
public sealed class ProtoSchemaException(string message) : FormatException(message);

/// <summary>
/// Message and enum types read from .proto files or a compiled descriptor set (protoc --descriptor_set_out), so
/// Protobuf bodies can be shown with their field names instead of numbers.
/// </summary>
public sealed class ProtoSchemaSet
{
    /// <summary>Deepest message nesting a schema may declare (the protobuf libraries' default recursion limit).</summary>
    public const int MaximumNesting = 100;

    private readonly Dictionary<string, ProtoMessageType> _messages;
    private readonly Dictionary<string, ProtoEnumType> _enums;

    private ProtoSchemaSet(IEnumerable<ProtoMessageType> messages, IEnumerable<ProtoEnumType> enums, IReadOnlyList<string> sources)
    {
        _messages = messages.GroupBy(message => message.FullName).ToDictionary(group => group.Key, group => group.First(), StringComparer.Ordinal);
        _enums = enums.GroupBy(item => item.FullName).ToDictionary(group => group.Key, group => group.First(), StringComparer.Ordinal);
        Sources = sources;
    }

    public static ProtoSchemaSet Empty { get; } = new([], [], []);

    /// <summary>Every message type but the generated map entries, in the order they were declared.</summary>
    public IReadOnlyList<ProtoMessageType> Messages => _messages.Values.Where(message => !message.IsMapEntry).ToArray();

    /// <summary>Every message type, map entries included, in the order they were declared (nested ones after their parent).</summary>
    public IReadOnlyList<ProtoMessageType> AllMessages => _messages.Values.ToArray();

    /// <summary>The files the types came from.</summary>
    public IReadOnlyList<string> Sources { get; }

    public bool IsEmpty => _messages.Count == 0;

    public ProtoMessageType? FindMessage(string fullName) => _messages.GetValueOrDefault(fullName.TrimStart('.'));

    public ProtoEnumType? FindEnum(string fullName) => _enums.GetValueOrDefault(fullName.TrimStart('.'));

    /// <summary>A message type by its full name ("orders.v1.OrderCreated") or, when that is unique, its short name.</summary>
    public ProtoMessageType? Resolve(string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return null;
        }
        name = name.Trim().TrimStart('.');
        if (name.StartsWith("type.googleapis.com/", StringComparison.Ordinal))
        {
            name = name["type.googleapis.com/".Length..];
        }
        if (FindMessage(name) is { } exact)
        {
            return exact;
        }
        var matches = Messages.Where(message => message.Name == name || message.FullName.EndsWith("." + name, StringComparison.Ordinal)).ToArray();
        return matches.Length == 1 ? matches[0] : null;
    }

    /// <summary>
    /// Reads a .proto file, a descriptor set, or every .proto, .desc, .pb and .protoset file of a folder and its
    /// subfolders; imports between the files are resolved by the types' full names.
    /// </summary>
    public static ProtoSchemaSet Load(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        var files = Directory.Exists(path)
            // One unreadable subfolder (a database volume, another account's cache) must not discard every schema beside it.
            ? Directory.EnumerateFiles(path, "*", new EnumerationOptions { RecurseSubdirectories = true, IgnoreInaccessible = true, AttributesToSkip = 0 })
                .Where(file => Path.GetExtension(file).ToLowerInvariant() is ".proto" or ".desc" or ".pb" or ".protoset" or ".binpb")
                .Order(StringComparer.Ordinal)
                .Take(2_000)
                .ToArray()
            : File.Exists(path)
                ? [path]
                : throw new ProtoSchemaException($"'{path}' is neither a file nor a folder.");
        if (files.Length == 0)
        {
            throw new ProtoSchemaException($"'{path}' holds no .proto files or descriptor sets.");
        }

        var texts = new List<(string Name, string Text)>();
        var sets = new List<ProtoSchemaSet>();
        foreach (var file in files)
        {
            if (Path.GetExtension(file).Equals(".proto", StringComparison.OrdinalIgnoreCase))
            {
                texts.Add((file, File.ReadAllText(file)));
            }
            else
            {
                sets.Add(FromDescriptorSet(File.ReadAllBytes(file), file));
            }
        }
        var parsed = texts.Count > 0 ? FromProtoFiles(texts) : Empty;
        return new ProtoSchemaSet(
            parsed._messages.Values.Concat(sets.SelectMany(set => set._messages.Values)),
            parsed._enums.Values.Concat(sets.SelectMany(set => set._enums.Values)),
            files);
    }

    /// <summary>Reads .proto source files (proto2 or proto3).</summary>
    public static ProtoSchemaSet FromProtoFiles(IEnumerable<(string Name, string Text)> files)
    {
        ArgumentNullException.ThrowIfNull(files);
        var declarations = new List<ProtoTextParser.MessageDeclaration>();
        var enums = new List<ProtoEnumType>();
        var names = new List<string>();
        foreach (var (name, text) in files)
        {
            names.Add(name);
            try
            {
                new ProtoTextParser(text).Parse(declarations, enums);
            }
            catch (ProtoSchemaException exception)
            {
                throw new ProtoSchemaException($"{Path.GetFileName(name)}: {exception.Message}");
            }
        }

        var messageNames = declarations.Select(message => message.FullName).ToHashSet(StringComparer.Ordinal);
        var enumNames = enums.Select(item => item.FullName).ToHashSet(StringComparer.Ordinal);
        var messages = declarations.Select(declaration => new ProtoMessageType(declaration.FullName, declaration.Fields.Select(field =>
        {
            if (field.Type is not null)
            {
                return new ProtoField(field.Name, field.Number, field.Type.Value, null, field.IsRepeated) { Oneof = field.Oneof };
            }
            // Protobuf scoping: the innermost enclosing scope that declares the name wins.
            var resolved = ResolveName(field.TypeReference!, declaration.FullName, candidate => messageNames.Contains(candidate) || enumNames.Contains(candidate));
            var type = resolved is not null && enumNames.Contains(resolved) ? ProtoFieldType.Enum : ProtoFieldType.Message;
            return new ProtoField(field.Name, field.Number, type, resolved ?? field.TypeReference, field.IsRepeated) { Oneof = field.Oneof };
        }).ToArray(), declaration.IsMapEntry));
        return new ProtoSchemaSet(messages, enums, names);
    }

    private static string? ResolveName(string reference, string scope, Func<string, bool> exists)
    {
        if (reference.StartsWith('.'))
        {
            return exists(reference[1..]) ? reference[1..] : null;
        }
        var current = scope;
        while (true)
        {
            var candidate = current.Length == 0 ? reference : $"{current}.{reference}";
            if (exists(candidate))
            {
                return candidate;
            }
            if (current.Length == 0)
            {
                return null;
            }
            var dot = current.LastIndexOf('.');
            current = dot < 0 ? string.Empty : current[..dot];
        }
    }

    /// <summary>Reads a FileDescriptorSet, as written by protoc --descriptor_set_out or buf build -o.</summary>
    public static ProtoSchemaSet FromDescriptorSet(byte[] data, string name = "descriptor set")
    {
        ArgumentNullException.ThrowIfNull(data);
        var messages = new List<ProtoMessageType>();
        var enums = new List<ProtoEnumType>();
        try
        {
            foreach (var file in Wire.Fields(data).Where(field => field.Number == 1 && field.Bytes is not null))
            {
                var package = Wire.Fields(file.Bytes!).Where(field => field.Number == 2).Select(field => Encoding.UTF8.GetString(field.Bytes!)).FirstOrDefault() ?? string.Empty;
                foreach (var field in Wire.Fields(file.Bytes!))
                {
                    if (field.Number == 4 && field.Bytes is not null)
                    {
                        ReadMessage(field.Bytes, package, messages, enums, 1);
                    }
                    else if (field.Number == 5 && field.Bytes is not null)
                    {
                        enums.Add(ReadEnum(field.Bytes, package));
                    }
                }
            }
        }
        catch (Exception exception) when (exception is IndexOutOfRangeException or ArgumentException or InvalidDataException)
        {
            throw new ProtoSchemaException($"{Path.GetFileName(name)} is not a descriptor set: {exception.Message}");
        }
        if (messages.Count == 0 && enums.Count == 0)
        {
            throw new ProtoSchemaException($"{Path.GetFileName(name)} holds no message types; is it a descriptor set (protoc --descriptor_set_out)?");
        }
        return new ProtoSchemaSet(messages, enums, [name]);
    }

    private static void ReadMessage(byte[] data, string scope, List<ProtoMessageType> messages, List<ProtoEnumType> enums, int depth)
    {
        // Recursion without a limit overflows the stack, which no handler can catch; protoc stops at 100 levels too.
        if (depth > ProtoSchemaSet.MaximumNesting)
            throw new InvalidDataException($"Messages are nested more than {ProtoSchemaSet.MaximumNesting} levels deep.");
        var fields = Wire.Fields(data).ToArray();
        var name = fields.Where(field => field.Number == 1 && field.Bytes is not null)
            .Select(field => Encoding.UTF8.GetString(field.Bytes!)).FirstOrDefault();
        if (string.IsNullOrWhiteSpace(name))
            throw new InvalidDataException("A descriptor message is missing its name.");
        var fullName = scope.Length == 0 ? name : $"{scope}.{name}";
        var isMapEntry = fields.Where(field => field.Number == 7 && field.Bytes is not null)
            .Any(options => Wire.Fields(options.Bytes!).Any(option => option.Number == 7 && option.Varint == 1));
        var declared = new List<ProtoField>();
        var oneofs = fields.Where(field => field.Number == 8 && field.Bytes is not null)
            .Select(field => Wire.Fields(field.Bytes!).Where(part => part.Number == 1 && part.Bytes is not null)
                .Select(part => Encoding.UTF8.GetString(part.Bytes!)).FirstOrDefault() ?? string.Empty).ToArray();
        foreach (var field in fields)
        {
            switch (field.Number)
            {
                case 2 when field.Bytes is not null:
                    var parts = Wire.Fields(field.Bytes).ToArray();
                    string Text(int number) => parts.Where(part => part.Number == number && part.Bytes is not null)
                        .Select(part => Encoding.UTF8.GetString(part.Bytes!)).FirstOrDefault() ?? string.Empty;
                    long Number(int number) => parts.Where(part => part.Number == number).Select(part => (long)part.Varint).FirstOrDefault();
                    var typeName = Text(6);
                    var oneofIndex = parts.FirstOrDefault(part => part.Number == 9 && part.WireType == 0);
                    var oneof = oneofIndex.Number == 9
                        ? oneofIndex.Varint < (ulong)oneofs.Length ? oneofs[(int)oneofIndex.Varint]
                            : throw new InvalidDataException("A field refers to an undeclared oneof.")
                        : null;
                    declared.Add(new ProtoField(Text(1), (int)Number(3), (ProtoFieldType)Number(5),
                        typeName.Length == 0 ? null : typeName.TrimStart('.'), Number(4) == 3) { Oneof = oneof });
                    break;
                case 3 when field.Bytes is not null:
                    ReadMessage(field.Bytes, fullName, messages, enums, depth + 1);
                    break;
                case 4 when field.Bytes is not null:
                    enums.Add(ReadEnum(field.Bytes, fullName));
                    break;
            }
        }
        messages.Add(new ProtoMessageType(fullName, declared, isMapEntry));
    }

    private static ProtoEnumType ReadEnum(byte[] data, string scope)
    {
        var name = string.Empty;
        var values = new Dictionary<int, string>();
        foreach (var field in Wire.Fields(data))
        {
            if (field.Number == 1 && field.Bytes is not null)
            {
                name = Encoding.UTF8.GetString(field.Bytes);
            }
            else if (field.Number == 2 && field.Bytes is not null)
            {
                var parts = Wire.Fields(field.Bytes).ToArray();
                var valueName = parts.Where(part => part.Number == 1).Select(part => Encoding.UTF8.GetString(part.Bytes!)).FirstOrDefault() ?? "?";
                var number = (int)parts.Where(part => part.Number == 2).Select(part => part.Varint).FirstOrDefault();
                values.TryAdd(number, valueName);
            }
        }
        return new ProtoEnumType(scope.Length == 0 ? name : $"{scope}.{name}", values);
    }

    /// <summary>Just enough of the wire format to read descriptors: varints and length-delimited fields.</summary>
    internal static class Wire
    {
        public readonly record struct Field(int Number, int WireType, ulong Varint, byte[]? Bytes);

        public static IEnumerable<Field> Fields(byte[] data)
        {
            var position = 0;
            while (position < data.Length)
            {
                var tag = ReadVarint(data, ref position);
                var number = (int)(tag >> 3);
                var wireType = (int)(tag & 7);
                switch (wireType)
                {
                    case 0:
                        yield return new Field(number, 0, ReadVarint(data, ref position), null);
                        break;
                    case 1:
                        position += 8;
                        break;
                    case 2:
                        var length = (int)ReadVarint(data, ref position);
                        if (length < 0 || position + length > data.Length)
                        {
                            throw new InvalidDataException("A length runs past the end.");
                        }
                        yield return new Field(number, 2, 0, data[position..(position + length)]);
                        position += length;
                        break;
                    case 5:
                        position += 4;
                        break;
                    default:
                        throw new InvalidDataException($"Wire type {wireType} is not used in descriptors.");
                }
                if (position > data.Length)
                {
                    throw new InvalidDataException("A field runs past the end.");
                }
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
}

/// <summary>Reads the declarations of a .proto file; options, services and extensions are skipped.</summary>
internal sealed class ProtoTextParser(string text)
{
    internal sealed record FieldDeclaration(string Name, int Number, ProtoFieldType? Type, string? TypeReference, bool IsRepeated)
    {
        public string? Oneof { get; init; }
    }

    internal sealed record MessageDeclaration(string FullName, List<FieldDeclaration> Fields, bool IsMapEntry);

    private static readonly Dictionary<string, ProtoFieldType> Scalars = new(StringComparer.Ordinal)
    {
        ["double"] = ProtoFieldType.Double, ["float"] = ProtoFieldType.Float, ["int64"] = ProtoFieldType.Int64,
        ["uint64"] = ProtoFieldType.UInt64, ["int32"] = ProtoFieldType.Int32, ["fixed64"] = ProtoFieldType.Fixed64,
        ["fixed32"] = ProtoFieldType.Fixed32, ["bool"] = ProtoFieldType.Bool, ["string"] = ProtoFieldType.String,
        ["bytes"] = ProtoFieldType.Bytes, ["uint32"] = ProtoFieldType.UInt32, ["sfixed32"] = ProtoFieldType.SFixed32,
        ["sfixed64"] = ProtoFieldType.SFixed64, ["sint32"] = ProtoFieldType.SInt32, ["sint64"] = ProtoFieldType.SInt64
    };

    private readonly List<string> _tokens = Tokenize(text);
    private int _position;
    private int _depth;
    private string _package = string.Empty;

    public void Parse(List<MessageDeclaration> messages, List<ProtoEnumType> enums)
    {
        while (_position < _tokens.Count)
        {
            var token = Next();
            switch (token)
            {
                case "syntax" or "edition" or "import" or "option":
                    SkipStatement();
                    break;
                case "package":
                    _package = Next();
                    Expect(";");
                    break;
                case "message":
                    ParseMessage(_package, messages, enums);
                    break;
                case "enum":
                    enums.Add(ParseEnum(_package));
                    break;
                case "service" or "extend":
                    Next();
                    SkipBlock();
                    break;
                case ";":
                    break;
                default:
                    throw new ProtoSchemaException($"Unexpected '{token}'.");
            }
        }
    }

    private void ParseMessage(string scope, List<MessageDeclaration> messages, List<ProtoEnumType> enums)
    {
        var name = Next();
        var fullName = scope.Length == 0 ? name : $"{scope}.{name}";
        var fields = new List<FieldDeclaration>();
        messages.Add(new MessageDeclaration(fullName, fields, false));
        Expect("{");
        ParseBody(fullName, fields, messages, enums);
    }

    private void ParseBody(string fullName, List<FieldDeclaration> fields, List<MessageDeclaration> messages, List<ProtoEnumType> enums, string? oneof = null)
    {
        // Messages and oneofs are parsed recursively; a stack overflow would end the process instead of reporting an error.
        if (++_depth > ProtoSchemaSet.MaximumNesting)
        {
            throw new ProtoSchemaException($"Messages are nested more than {ProtoSchemaSet.MaximumNesting} levels deep.");
        }
        try
        {
            ParseBodyLevel(fullName, fields, messages, enums, oneof);
        }
        finally
        {
            _depth--;
        }
    }

    private void ParseBodyLevel(string fullName, List<FieldDeclaration> fields, List<MessageDeclaration> messages, List<ProtoEnumType> enums, string? oneof)
    {
        while (true)
        {
            var token = Next();
            switch (token)
            {
                case "}":
                    return;
                case ";":
                    continue;
                case "message":
                    ParseMessage(fullName, messages, enums);
                    continue;
                case "enum":
                    enums.Add(ParseEnum(fullName));
                    continue;
                case "option" or "reserved" or "extensions":
                    SkipStatement();
                    continue;
                case "extend":
                    Next();
                    SkipBlock();
                    continue;
                case "oneof":
                    var groupName = Next();
                    Expect("{");
                    // A oneof's fields belong to the message; options inside are skipped.
                    ParseBody(fullName, fields, messages, enums, groupName);
                    continue;
                case "map":
                    Expect("<");
                    var keyType = Next();
                    Expect(",");
                    var valueType = Next();
                    Expect(">");
                    var mapName = Next();
                    Expect("=");
                    var mapNumber = ParseNumber();
                    SkipFieldOptions();
                    var entryName = $"{fullName}.{char.ToUpperInvariant(mapName[0])}{mapName[1..]}Entry";
                    messages.Add(new MessageDeclaration(entryName, [Field("key", 1, keyType, false), Field("value", 2, valueType, false)], true));
                    fields.Add(new FieldDeclaration(mapName, mapNumber, null, "." + entryName, true));
                    continue;
                case "group":
                    throw new ProtoSchemaException("proto2 groups are not supported.");
            }

            var repeated = false;
            if (token is "repeated" or "optional" or "required")
            {
                repeated = token == "repeated";
                token = Next();
            }
            var fieldName = Next();
            Expect("=");
            var number = ParseNumber();
            SkipFieldOptions();
            fields.Add(Field(fieldName, number, token, repeated) with { Oneof = oneof });
        }
    }

    private static FieldDeclaration Field(string name, int number, string type, bool repeated) =>
        Scalars.TryGetValue(type, out var scalar)
            ? new FieldDeclaration(name, number, scalar, null, repeated)
            : new FieldDeclaration(name, number, null, type, repeated);

    private ProtoEnumType ParseEnum(string scope)
    {
        var name = Next();
        Expect("{");
        var values = new Dictionary<int, string>();
        while (true)
        {
            var token = Next();
            if (token == "}")
            {
                break;
            }
            if (token is ";")
            {
                continue;
            }
            if (token is "option" or "reserved")
            {
                SkipStatement();
                continue;
            }
            Expect("=");
            var negative = Peek() == "-";
            if (negative)
            {
                Next();
            }
            var number = ParseEnumNumber(negative);
            values.TryAdd(number, token);
            SkipFieldOptions();
        }
        return new ProtoEnumType(scope.Length == 0 ? name : $"{scope}.{name}", values);
    }

    private int ParseNumber()
    {
        var token = Next();
        var hexadecimal = token.StartsWith("0x", StringComparison.OrdinalIgnoreCase);
        if (int.TryParse(hexadecimal ? token[2..] : token,
                hexadecimal ? System.Globalization.NumberStyles.AllowHexSpecifier : System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture, out var number) && number >= 0)
            return number;
        throw new ProtoSchemaException($"Expected a field number, not '{token}'.");
    }

    private int ParseEnumNumber(bool negative)
    {
        var token = Next();
        var hexadecimal = token.StartsWith("0x", StringComparison.OrdinalIgnoreCase);
        // The magnitude of int.MinValue is one greater than int.MaxValue. Parse it unsigned
        // before applying the separately consumed sign; hex parsing must not reinterpret bits
        // as a signed number. Field numbers retain their independent nonnegative validation.
        if (ulong.TryParse(hexadecimal ? token[2..] : token,
                hexadecimal ? System.Globalization.NumberStyles.AllowHexSpecifier : System.Globalization.NumberStyles.None,
                System.Globalization.CultureInfo.InvariantCulture, out var magnitude) &&
            magnitude <= (negative ? 2147483648UL : int.MaxValue))
            return (int)(negative ? -(long)magnitude : (long)magnitude);
        throw new ProtoSchemaException($"Expected an int32 enum value, not '{(negative ? "-" : string.Empty)}{token}'.");
    }

    /// <summary>Skips "[deprecated = true]" after a field, then its semicolon.</summary>
    private void SkipFieldOptions()
    {
        if (Peek() == "[")
        {
            var depth = 0;
            do
            {
                var token = Next();
                depth += token == "[" ? 1 : token == "]" ? -1 : 0;
            }
            while (depth > 0);
        }
        Expect(";");
    }

    private void SkipStatement()
    {
        var depth = 0;
        while (true)
        {
            var token = Next();
            depth += token is "{" ? 1 : token is "}" ? -1 : 0;
            if (token == ";" && depth == 0 || token == "}" && depth == 0)
            {
                return;
            }
        }
    }

    private void SkipBlock()
    {
        while (Peek() != "{")
        {
            Next();
        }
        var depth = 0;
        do
        {
            var token = Next();
            depth += token == "{" ? 1 : token == "}" ? -1 : 0;
        }
        while (depth > 0);
    }

    private string Next() => _position < _tokens.Count ? _tokens[_position++] : throw new ProtoSchemaException("The file ends too early.");

    private string? Peek() => _position < _tokens.Count ? _tokens[_position] : null;

    private void Expect(string expected)
    {
        var token = Next();
        if (token != expected)
        {
            throw new ProtoSchemaException($"Expected '{expected}', not '{token}'.");
        }
    }

    private static List<string> Tokenize(string source)
    {
        var tokens = new List<string>();
        var position = 0;
        while (position < source.Length)
        {
            var current = source[position];
            if (char.IsWhiteSpace(current))
            {
                position++;
            }
            else if (current == '/' && position + 1 < source.Length && source[position + 1] == '/')
            {
                while (position < source.Length && source[position] != '\n')
                {
                    position++;
                }
            }
            else if (current == '/' && position + 1 < source.Length && source[position + 1] == '*')
            {
                var end = source.IndexOf("*/", position + 2, StringComparison.Ordinal);
                position = end < 0 ? source.Length : end + 2;
            }
            else if (current is '"' or '\'')
            {
                var start = position++;
                while (position < source.Length && source[position] != current)
                {
                    position += source[position] == '\\' ? 2 : 1;
                }
                position = Math.Min(position + 1, source.Length);
                tokens.Add(source[start..position]);
            }
            else if (char.IsLetterOrDigit(current) || current is '_' or '.')
            {
                var start = position;
                while (position < source.Length && (char.IsLetterOrDigit(source[position]) || source[position] is '_' or '.'))
                {
                    position++;
                }
                tokens.Add(source[start..position]);
            }
            else
            {
                tokens.Add(current.ToString());
                position++;
            }
        }
        return tokens;
    }
}

/// <summary>
/// The .proto files QueueLoom was pointed at (Settings → Protobuf schemas), shared by every view that decodes
/// bodies; empty until a folder or file is loaded.
/// </summary>
public static class ProtoSchemaCatalog
{
    private static volatile ProtoSchemaSet _current = ProtoSchemaSet.Empty;

    public static ProtoSchemaSet Current
    {
        get => _current;
        set => _current = value ?? ProtoSchemaSet.Empty;
    }

    /// <summary>Message type names a sender may leave in the content type or a property.</summary>
    private static readonly string[] HintNames = ["messageType", "message-type", "proto", "protoType", "proto-type", "proto_message", "type"];

    /// <summary>
    /// The message type a message names: a content type parameter (application/x-protobuf; messageType=orders.Order),
    /// or a property such as messageType; null when it names none.
    /// </summary>
    public static string? HintFrom(string? contentType, IEnumerable<MessageApplicationProperty>? properties = null)
    {
        if (!string.IsNullOrWhiteSpace(contentType))
        {
            foreach (var parameter in contentType.Split(';').Skip(1))
            {
                var parts = parameter.Split('=', 2, StringSplitOptions.TrimEntries);
                if (parts.Length == 2 && HintNames.Contains(parts[0], StringComparer.OrdinalIgnoreCase) && parts[1].Trim('"').Length > 0)
                {
                    return parts[1].Trim('"');
                }
            }
        }
        foreach (var name in HintNames)
        {
            var property = properties?.FirstOrDefault(item => string.Equals(item.Name, name, StringComparison.OrdinalIgnoreCase));
            if (property is { Value.Length: > 0 })
            {
                return property.Value;
            }
        }
        return null;
    }
}
