using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace QueueLoom.Core.ServiceBus;

/// <summary>A search that cannot be read, with the reason (a bad regular expression, a broken JSON path…).</summary>
public sealed class MessageSearchQueryException(string message) : ArgumentException(message);

/// <summary>
/// What a dead-letter search looks for, in one of three forms:
/// <list type="bullet">
/// <item>plain text, found anywhere (IDs, subject, reason, properties, body), ignoring case;</item>
/// <item><c>/regex/</c>, a regular expression over the same fields (add <c>i</c> after the last slash to ignore case);</item>
/// <item><c>$.order.status == 'failed'</c>, a condition on a field of the JSON body (also inside gzip or base64),
/// with ==, !=, &gt;, &gt;=, &lt;, &lt;=, =~ /regex/, or the path alone for "has the field"; conditions join with
/// <c>and</c> / <c>or</c> (or &amp;&amp; / ||). <c>[0]</c> picks an item of a list and <c>[*]</c> any item.</item>
/// </list>
/// </summary>
public sealed partial class MessageSearchQuery
{
    private static readonly TimeSpan RegexTimeout = TimeSpan.FromMilliseconds(250);

    private readonly string? _text;
    private readonly Regex? _regex;
    private readonly IReadOnlyList<IReadOnlyList<JsonCondition>>? _json;

    private MessageSearchQuery(string query, string? text, Regex? regex, IReadOnlyList<IReadOnlyList<JsonCondition>>? json)
    {
        Query = query;
        _text = text;
        _regex = regex;
        _json = json;
    }

    public string Query { get; }

    public bool IsRegex => _regex is not null;

    public bool IsJsonCondition => _json is not null;

    /// <summary>Reads a search; throws <see cref="MessageSearchQueryException"/> with the reason when it cannot.</summary>
    public static MessageSearchQuery Parse(string query)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(query);
        var trimmed = query.Trim();
        if (RegexLiteral().Match(trimmed) is { Success: true } literal)
        {
            return new MessageSearchQuery(trimmed, null, CreateRegex(literal.Groups["pattern"].Value, literal.Groups["flags"].Value), null);
        }
        if (trimmed.StartsWith("$.", StringComparison.Ordinal) || trimmed.StartsWith("$[", StringComparison.Ordinal))
        {
            return new MessageSearchQuery(trimmed, null, null, new JsonQueryParser(trimmed).Parse());
        }
        return new MessageSearchQuery(trimmed, trimmed, null, null);
    }

    public bool Matches(BrowsedMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        var properties = message.Properties;
        return Matches(
            [properties.CorrelationId, properties.MessageId, properties.Subject, properties.ContentType, properties.SessionId,
                properties.To, properties.ReplyTo, message.DeadLetterReason, message.DeadLetterErrorDescription],
            message.ApplicationProperties.Select(property => new KeyValuePair<string, string?>(property.Name, property.Value)),
            message.Body,
            properties.ContentType);
    }

    /// <summary>Matches the message's text fields, its properties (name and value) and its body.</summary>
    public bool Matches(IEnumerable<string?> fields, IEnumerable<KeyValuePair<string, string?>> properties, ReadOnlyMemory<byte> body,
        string? contentType = null)
    {
        ArgumentNullException.ThrowIfNull(fields);
        ArgumentNullException.ThrowIfNull(properties);
        if (_json is not null)
        {
            return MatchesJson(body, contentType);
        }

        foreach (var field in fields)
        {
            if (IsTextMatch(field))
            {
                return true;
            }
        }
        foreach (var (name, value) in properties)
        {
            if (IsTextMatch(name) || IsTextMatch(value))
            {
                return true;
            }
        }
        return body.Length > 0 && IsTextMatch(Encoding.UTF8.GetString(body.Span));
    }

    private bool IsTextMatch(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return false;
        }
        if (_regex is null)
        {
            return value.Contains(_text!, StringComparison.OrdinalIgnoreCase);
        }
        try
        {
            return _regex.IsMatch(value);
        }
        catch (RegexMatchTimeoutException)
        {
            return false;
        }
    }

    private bool MatchesJson(ReadOnlyMemory<byte> body, string? contentType)
    {
        if (body.Length == 0)
        {
            return false;
        }
        JsonDocument? document = null;
        try
        {
            try
            {
                document = JsonDocument.Parse(body);
            }
            catch (JsonException)
            {
                // Packed bodies (gzip, base64, Avro…) are searched in their decoded JSON.
                if (BodyDecoder.Decode(body, contentType) is { IsJson: true } decoded)
                {
                    document = JsonDocument.Parse(decoded.Text);
                }
            }
            if (document is null)
            {
                return false;
            }
            var root = document.RootElement;
            return _json!.Any(all => all.All(condition => condition.IsTrue(root)));
        }
        catch (JsonException)
        {
            return false;
        }
        finally
        {
            document?.Dispose();
        }
    }

    private static Regex CreateRegex(string pattern, string flags)
    {
        var options = RegexOptions.CultureInvariant;
        foreach (var flag in flags)
        {
            options |= flag switch
            {
                'i' => RegexOptions.IgnoreCase,
                'm' => RegexOptions.Multiline,
                's' => RegexOptions.Singleline,
                'x' => RegexOptions.IgnorePatternWhitespace,
                _ => throw new MessageSearchQueryException($"/{pattern}/{flags}: '{flag}' is not a flag; use i, m, s or x.")
            };
        }
        try
        {
            return new Regex(pattern, options, RegexTimeout);
        }
        catch (ArgumentException exception)
        {
            throw new MessageSearchQueryException($"The regular expression cannot be read: {exception.Message}");
        }
    }

    [GeneratedRegex(@"^/(?<pattern>.+)/(?<flags>[imsx]*)$", RegexOptions.Singleline)]
    private static partial Regex RegexLiteral();

    private enum Operator
    {
        Exists,
        Equal,
        NotEqual,
        Greater,
        GreaterOrEqual,
        Less,
        LessOrEqual,
        Matches
    }

    private abstract record PathStep;

    private sealed record Property(string Name) : PathStep;

    private sealed record Index(int Position) : PathStep;

    private sealed record AnyItem : PathStep;

    /// <summary>A path and what its value must be; any one value the path reaches (through [*]) is enough.</summary>
    private sealed record JsonCondition(IReadOnlyList<PathStep> Path, Operator Op, JsonElement? Value, Regex? Pattern)
    {
        public bool IsTrue(JsonElement root)
        {
            var values = Resolve(root).ToArray();
            return Op == Operator.NotEqual
                ? values.Length > 0 && values.All(value => !AreEqual(value, Value!.Value))
                : values.Any(Satisfies);
        }

        private IEnumerable<JsonElement> Resolve(JsonElement root)
        {
            IEnumerable<JsonElement> current = [root];
            foreach (var step in Path)
            {
                current = current.SelectMany(element => step switch
                {
                    Property property when element.ValueKind == JsonValueKind.Object && element.TryGetProperty(property.Name, out var child) => [child],
                    Index index when element.ValueKind == JsonValueKind.Array && index.Position < element.GetArrayLength() =>
                        [element[index.Position]],
                    AnyItem when element.ValueKind == JsonValueKind.Array => element.EnumerateArray().ToArray(),
                    AnyItem when element.ValueKind == JsonValueKind.Object => element.EnumerateObject().Select(item => item.Value).ToArray(),
                    _ => Array.Empty<JsonElement>()
                });
            }
            return current;
        }

        private bool Satisfies(JsonElement value) => Op switch
        {
            Operator.Exists => true,
            Operator.Equal => AreEqual(value, Value!.Value),
            Operator.Matches => value.ValueKind is JsonValueKind.String or JsonValueKind.Number or JsonValueKind.True or JsonValueKind.False &&
                                SafeMatch(Pattern!, value.ValueKind == JsonValueKind.String ? value.GetString()! : value.GetRawText()),
            _ => Compare(value, Value!.Value) is { } order && Op switch
            {
                Operator.Greater => order > 0,
                Operator.GreaterOrEqual => order >= 0,
                Operator.Less => order < 0,
                _ => order <= 0
            }
        };

        private static bool SafeMatch(Regex regex, string text)
        {
            try
            {
                return regex.IsMatch(text);
            }
            catch (RegexMatchTimeoutException)
            {
                return false;
            }
        }

        /// <summary>Same type and value; a number also equals text holding that number ("250" and 250).</summary>
        private static bool AreEqual(JsonElement value, JsonElement expected) => (value.ValueKind, expected.ValueKind) switch
        {
            (JsonValueKind.String, JsonValueKind.String) => value.GetString() == expected.GetString(),
            (JsonValueKind.Number, JsonValueKind.Number) => value.GetDouble() == expected.GetDouble(),
            (JsonValueKind.String, JsonValueKind.Number) => double.TryParse(value.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out var number) &&
                                                            number == expected.GetDouble(),
            (JsonValueKind.True or JsonValueKind.False, JsonValueKind.True or JsonValueKind.False) => value.ValueKind == expected.ValueKind,
            (JsonValueKind.Null, JsonValueKind.Null) => true,
            _ => false
        };

        /// <summary>Numbers by value, text in ordinal order (so ISO dates compare as dates); null when they cannot be ordered.</summary>
        private static int? Compare(JsonElement value, JsonElement bound)
        {
            if (bound.ValueKind == JsonValueKind.Number)
            {
                double? number = value.ValueKind switch
                {
                    JsonValueKind.Number => value.GetDouble(),
                    JsonValueKind.String when double.TryParse(value.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed) => parsed,
                    _ => null
                };
                return number?.CompareTo(bound.GetDouble());
            }
            return value.ValueKind == JsonValueKind.String && bound.ValueKind == JsonValueKind.String
                ? Math.Sign(string.CompareOrdinal(value.GetString(), bound.GetString()))
                : null;
        }
    }

    /// <summary>Reads "$.a.b[0] == 'x' and $.c > 5 or $.d" into OR-groups of AND-ed conditions.</summary>
    private sealed class JsonQueryParser(string text)
    {
        private int _position;

        public IReadOnlyList<IReadOnlyList<JsonCondition>> Parse()
        {
            var groups = new List<IReadOnlyList<JsonCondition>>();
            var current = new List<JsonCondition> { ParseCondition() };
            while (true)
            {
                SkipSpaces();
                if (_position >= text.Length)
                {
                    break;
                }
                if (TryWord("and") || TryText("&&"))
                {
                    current.Add(ParseCondition());
                }
                else if (TryWord("or") || TryText("||"))
                {
                    groups.Add(current);
                    current = [ParseCondition()];
                }
                else
                {
                    throw Error($"Expected and, or, or the end after the condition, not '{text[_position..]}'.");
                }
            }
            groups.Add(current);
            return groups;
        }

        private JsonCondition ParseCondition()
        {
            SkipSpaces();
            var path = ParsePath();
            SkipSpaces();
            var op = TryText("==") ? Operator.Equal
                : TryText("!=") ? Operator.NotEqual
                : TryText(">=") ? Operator.GreaterOrEqual
                : TryText("<=") ? Operator.LessOrEqual
                : TryText("=~") ? Operator.Matches
                : TryText(">") ? Operator.Greater
                : TryText("<") ? Operator.Less
                : TryText("=") ? Operator.Equal
                : Operator.Exists;
            if (op == Operator.Exists)
            {
                return new JsonCondition(path, op, null, null);
            }
            SkipSpaces();
            if (op == Operator.Matches)
            {
                return new JsonCondition(path, op, null, ParseRegex());
            }
            var value = ParseValue();
            if (op is Operator.Greater or Operator.GreaterOrEqual or Operator.Less or Operator.LessOrEqual &&
                value.ValueKind is not (JsonValueKind.Number or JsonValueKind.String))
            {
                throw Error("Only numbers and text can be compared with <, <=, > or >=.");
            }
            return new JsonCondition(path, op, value, null);
        }

        private IReadOnlyList<PathStep> ParsePath()
        {
            if (!TryText("$"))
            {
                throw Error("A condition starts with a path such as $.order.status.");
            }
            var steps = new List<PathStep>();
            while (_position < text.Length)
            {
                if (TryText("."))
                {
                    var start = _position;
                    while (_position < text.Length && (char.IsLetterOrDigit(text[_position]) || text[_position] is '_' or '-' or '$' or '@'))
                    {
                        _position++;
                    }
                    if (start == _position)
                    {
                        if (TryText("*"))
                        {
                            steps.Add(new AnyItem());
                            continue;
                        }
                        throw Error("Expected a field name after '.'; write other names as ['name'].");
                    }
                    steps.Add(new Property(text[start.._position]));
                }
                else if (TryText("["))
                {
                    SkipSpaces();
                    if (TryText("*"))
                    {
                        steps.Add(new AnyItem());
                    }
                    else if (_position < text.Length && text[_position] is '\'' or '"')
                    {
                        steps.Add(new Property(ParseString()));
                    }
                    else
                    {
                        var start = _position;
                        while (_position < text.Length && char.IsDigit(text[_position]))
                        {
                            _position++;
                        }
                        if (start == _position)
                        {
                            throw Error("Expected a number, * or a 'quoted name' inside [ ].");
                        }
                        steps.Add(new Index(int.Parse(text[start.._position], CultureInfo.InvariantCulture)));
                    }
                    SkipSpaces();
                    if (!TryText("]"))
                    {
                        throw Error("A ']' is missing.");
                    }
                }
                else
                {
                    break;
                }
            }
            if (steps.Count == 0)
            {
                throw Error("The path names no field; for example $.order.status.");
            }
            return steps;
        }

        private JsonElement ParseValue()
        {
            if (_position < text.Length && text[_position] is '\'' or '"')
            {
                return JsonSerializer.SerializeToElement(ParseString());
            }
            var start = _position;
            while (_position < text.Length && !char.IsWhiteSpace(text[_position]) && text[_position] is not ('&' or '|'))
            {
                _position++;
            }
            var word = text[start.._position];
            if (word is "true" or "false" or "null" ||
                double.TryParse(word, NumberStyles.Float, CultureInfo.InvariantCulture, out _))
            {
                return JsonDocument.Parse(word).RootElement.Clone();
            }
            throw Error(word.Length == 0
                ? "A value is missing after the comparison."
                : $"'{word}' is not a value: write text in quotes ('failed'), or a number, true, false or null.");
        }

        private Regex ParseRegex()
        {
            if (!TryText("/"))
            {
                throw Error("=~ takes a regular expression between slashes, such as /^ORD-\\d+$/.");
            }
            var end = text.IndexOf('/', _position);
            while (end > 0 && text[end - 1] == '\\')
            {
                end = text.IndexOf('/', end + 1);
            }
            if (end < 0)
            {
                throw Error("The closing '/' of the regular expression is missing.");
            }
            var pattern = text[_position..end];
            _position = end + 1;
            var flagsStart = _position;
            while (_position < text.Length && char.IsAsciiLetterLower(text[_position]))
            {
                _position++;
            }
            return CreateRegex(pattern, text[flagsStart.._position]);
        }

        private string ParseString()
        {
            var quote = text[_position++];
            var value = new StringBuilder();
            while (_position < text.Length && text[_position] != quote)
            {
                if (text[_position] == '\\' && _position + 1 < text.Length)
                {
                    _position++;
                }
                value.Append(text[_position++]);
            }
            if (_position >= text.Length)
            {
                throw Error("A closing quote is missing.");
            }
            _position++;
            return value.ToString();
        }

        private bool TryText(string expected)
        {
            if (string.CompareOrdinal(text, _position, expected, 0, expected.Length) == 0)
            {
                _position += expected.Length;
                return true;
            }
            return false;
        }

        private bool TryWord(string word)
        {
            if (_position + word.Length <= text.Length &&
                string.Compare(text, _position, word, 0, word.Length, StringComparison.OrdinalIgnoreCase) == 0 &&
                (_position + word.Length == text.Length || char.IsWhiteSpace(text[_position + word.Length])))
            {
                _position += word.Length;
                return true;
            }
            return false;
        }

        private void SkipSpaces()
        {
            while (_position < text.Length && char.IsWhiteSpace(text[_position]))
            {
                _position++;
            }
        }

        private MessageSearchQueryException Error(string message) => new($"At character {_position + 1}: {message}");
    }
}
