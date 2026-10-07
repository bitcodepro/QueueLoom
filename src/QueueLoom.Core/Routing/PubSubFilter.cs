using System.Runtime.CompilerServices;
using System.Text;

namespace QueueLoom.Core.Routing;

/// <summary>
/// Google Cloud Pub/Sub subscription filters on message attributes: <c>attributes:name</c> (has the attribute),
/// <c>attributes.name = "value"</c>, <c>attributes.name != "value"</c> (also true when the attribute is missing),
/// <c>hasPrefix(attributes.name, "prefix")</c>, joined with <c>AND</c>, <c>OR</c>, <c>NOT</c> (or <c>-</c>) and
/// parentheses. AND and OR are not mixed without parentheses. Names and values are case-sensitive, as the
/// Pub/Sub emulator confirms.
/// </summary>
public sealed class PubSubFilter
{
    private readonly Node _root;

    private PubSubFilter(Node root) => _root = root;

    /// <summary>Reads a filter; throws <see cref="SqlFilterSyntaxException"/> with the position of a mistake.</summary>
    public static PubSubFilter Parse(string filter)
    {
        var parser = new Parser(filter ?? string.Empty);
        return new PubSubFilter(parser.ParseAll());
    }

    /// <summary>Whether the filter lets the message through; <paramref name="steps"/> gets every comparison and its result.</summary>
    public bool Evaluate(RoutingMessage message, List<SqlFilterStep>? steps = null)
    {
        ArgumentNullException.ThrowIfNull(message);
        return _root.Evaluate(message, steps);
    }

    private abstract record Node
    {
        // Conditions can chain or nest deeply enough to exhaust the stack, which would end the process. Running short
        // of stack is reported instead.
        public bool Evaluate(RoutingMessage message, List<SqlFilterStep>? steps) => RuntimeHelpers.TryEnsureSufficientExecutionStack()
            ? EvaluateCore(message, steps)
            : throw new SqlFilterNotSupportedException("The filter is nested too deeply to evaluate.");

        protected abstract bool EvaluateCore(RoutingMessage message, List<SqlFilterStep>? steps);
    }

    private sealed record And(Node Left, Node Right) : Node
    {
        // Both sides are evaluated so every failing comparison can be explained.
        protected override bool EvaluateCore(RoutingMessage message, List<SqlFilterStep>? steps) =>
            Left.Evaluate(message, steps) & Right.Evaluate(message, steps);
    }

    private sealed record Or(Node Left, Node Right) : Node
    {
        protected override bool EvaluateCore(RoutingMessage message, List<SqlFilterStep>? steps) =>
            Left.Evaluate(message, steps) | Right.Evaluate(message, steps);
    }

    private sealed record Not(Node Operand, string Text) : Node
    {
        protected override bool EvaluateCore(RoutingMessage message, List<SqlFilterStep>? steps)
        {
            var inner = new List<SqlFilterStep>();
            var result = !Operand.Evaluate(message, inner);
            steps?.Add(new SqlFilterStep(Text, result, inner.Count == 1 ? inner[0].Actual : null));
            return result;
        }
    }

    private enum Comparison
    {
        Has,
        Equal,
        NotEqual,
        Prefix
    }

    private sealed record Attribute(string Name, Comparison Kind, string Value, string Text) : Node
    {
        protected override bool EvaluateCore(RoutingMessage message, List<SqlFilterStep>? steps)
        {
            var exists = message.Attributes.ContainsKey(Name);
            var actual = exists ? message.AttributeTextOf(Name) : null;
            var result = Kind switch
            {
                Comparison.Has => exists,
                Comparison.Equal => exists && string.Equals(actual, Value, StringComparison.Ordinal),
                Comparison.NotEqual => !exists || !string.Equals(actual, Value, StringComparison.Ordinal),
                _ => exists && actual!.StartsWith(Value, StringComparison.Ordinal)
            };
            steps?.Add(new SqlFilterStep(Text, result, exists ? $"{Name} is {Quote(actual!)}" : $"{Name} is missing"));
            return result;
        }
    }

    private static string Quote(string value) => "\"" + value.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("\"", "\\\"", StringComparison.Ordinal) + "\"";

    private sealed class Parser(string text)
    {
        private int _position;

        public Node ParseAll()
        {
            SkipSpaces();
            if (_position >= text.Length)
            {
                throw new SqlFilterSyntaxException("The filter is empty.", 0);
            }
            var node = ParseExpression();
            SkipSpaces();
            if (_position < text.Length)
            {
                throw new SqlFilterSyntaxException($"Unexpected '{text[_position]}'.", _position);
            }
            return node;
        }

        private Node ParseExpression()
        {
            var left = ParseUnary();
            string? joiner = null;
            while (true)
            {
                SkipSpaces();
                var start = _position;
                var word = TryKeyword("AND") ? "AND" : TryKeyword("OR") ? "OR" : null;
                if (word is null)
                {
                    return left;
                }
                if (joiner is not null && joiner != word)
                {
                    throw new SqlFilterSyntaxException("Pub/Sub needs parentheses to mix AND with OR.", start);
                }
                joiner = word;
                var right = ParseUnary();
                left = word == "AND" ? new And(left, right) : new Or(left, right);
            }
        }

        // Every nesting step (NOT, '-', parentheses) passes through here; deep enough nesting is a syntax error, not a
        // stack overflow that would end the process.
        private Node ParseUnary()
        {
            if (!RuntimeHelpers.TryEnsureSufficientExecutionStack())
            {
                throw new SqlFilterSyntaxException("The filter is nested too deeply.", _position);
            }
            SkipSpaces();
            var start = _position;
            if (TryKeyword("NOT") || TryChar('-'))
            {
                var operand = ParseUnary();
                return new Not(operand, text[start.._position].Trim());
            }
            if (TryChar('('))
            {
                var inner = ParseExpression();
                SkipSpaces();
                if (!TryChar(')'))
                {
                    throw new SqlFilterSyntaxException("A ')' is missing.", _position);
                }
                return inner;
            }
            if (TryKeyword("hasPrefix"))
            {
                SkipSpaces();
                Expect('(');
                var name = ParseAttributeName('.');
                SkipSpaces();
                Expect(',');
                var prefix = ParseString();
                SkipSpaces();
                Expect(')');
                return new Attribute(name, Comparison.Prefix, prefix, text[start.._position].Trim());
            }

            SkipSpaces();
            var attributeStart = _position;
            if (!TryWord("attributes"))
            {
                throw new SqlFilterSyntaxException("Expected attributes.name, attributes:name, hasPrefix(…), NOT or '('.", _position);
            }
            if (TryChar(':'))
            {
                var name = ParseName();
                return new Attribute(name, Comparison.Has, string.Empty, text[attributeStart.._position]);
            }
            _position = attributeStart;
            var attribute = ParseAttributeName('.');
            SkipSpaces();
            Comparison kind;
            if (TryChar('!'))
            {
                Expect('=');
                kind = Comparison.NotEqual;
            }
            else if (TryChar('='))
            {
                kind = Comparison.Equal;
            }
            else
            {
                throw new SqlFilterSyntaxException("Expected = or != after the attribute.", _position);
            }
            var value = ParseString();
            return new Attribute(attribute, kind, value, text[attributeStart.._position].Trim());
        }

        private string ParseAttributeName(char separator)
        {
            SkipSpaces();
            if (!TryWord("attributes"))
            {
                throw new SqlFilterSyntaxException("Expected attributes.name.", _position);
            }
            Expect(separator);
            return ParseName();
        }

        private string ParseName()
        {
            if (_position < text.Length && text[_position] == '"')
            {
                return ParseString();
            }
            var start = _position;
            while (_position < text.Length && (char.IsLetterOrDigit(text[_position]) || text[_position] is '_' or '-'))
            {
                _position++;
            }
            if (start == _position)
            {
                throw new SqlFilterSyntaxException("Expected an attribute name; write names with characters other than letters, digits, - and _ in \"quotes\".", _position);
            }
            return text[start.._position];
        }

        private string ParseString()
        {
            SkipSpaces();
            if (_position >= text.Length || text[_position] is not ('"' or '\''))
            {
                throw new SqlFilterSyntaxException("Expected a \"quoted\" value.", _position);
            }
            var quote = text[_position++];
            var value = new StringBuilder();
            while (_position < text.Length && text[_position] != quote)
            {
                if (text[_position] == '\\')
                {
                    // Google documents escapes such as \u0045, while the Pub/Sub emulator compares the backslash as it
                    // is; with the two disagreeing, Pub/Sub has the last word instead of a guess.
                    throw new SqlFilterNotSupportedException(
                        "The filter uses a backslash escape, which QueueLoom cannot read the way Pub/Sub does; Pub/Sub decides.");
                }
                value.Append(text[_position++]);
            }
            if (_position >= text.Length)
            {
                throw new SqlFilterSyntaxException("A closing quote is missing.", _position);
            }
            _position++;
            return value.ToString();
        }

        private void Expect(char expected)
        {
            SkipSpaces();
            if (!TryChar(expected))
            {
                throw new SqlFilterSyntaxException($"Expected '{expected}'.", _position);
            }
        }

        private bool TryChar(char expected)
        {
            if (_position < text.Length && text[_position] == expected)
            {
                _position++;
                return true;
            }
            return false;
        }

        private bool TryWord(string word)
        {
            if (string.CompareOrdinal(text, _position, word, 0, word.Length) == 0 &&
                (_position + word.Length >= text.Length || !char.IsLetterOrDigit(text[_position + word.Length])))
            {
                _position += word.Length;
                return true;
            }
            return false;
        }

        private bool TryKeyword(string word)
        {
            SkipSpaces();
            if (_position + word.Length <= text.Length &&
                string.Compare(text, _position, word, 0, word.Length, StringComparison.Ordinal) == 0 &&
                (_position + word.Length == text.Length || !char.IsLetterOrDigit(text[_position + word.Length]) && text[_position + word.Length] != '_'))
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
    }
}
