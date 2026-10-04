using System.Globalization;
using System.Text;

namespace QueueLoom.Core.Routing;

/// <summary>The expression uses something QueueLoom cannot evaluate here; Service Bus itself still can.</summary>
public sealed class SqlFilterNotSupportedException(string message) : Exception(message);

/// <summary>The expression is not valid Service Bus SQL filter syntax.</summary>
public sealed class SqlFilterSyntaxException(string message, int position) : Exception(message)
{
    public int Position { get; } = position;
}

/// <summary>One comparison inside a filter and what it gave for a message, for explaining why a rule did not match.</summary>
public sealed record SqlFilterStep(string Text, bool? Result, string? Actual);

/// <summary>
/// The SQL filter language of Azure Service Bus subscription rules, parsed and evaluated against a message: comparisons,
/// AND / OR / NOT, LIKE with ESCAPE, IN, IS NULL, EXISTS, arithmetic and the sys. and user. property scopes. Missing
/// properties give "unknown", which, as in Service Bus, never matches.
/// </summary>
public sealed class SqlFilter
{
    private readonly Node _root;

    private SqlFilter(string text, Node root)
    {
        Text = text;
        _root = root;
    }

    public string Text { get; }

    public static SqlFilter Parse(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        var parser = new Parser(text);
        var root = parser.ParseAll();
        return new SqlFilter(text, root);
    }

    /// <summary>True, false, or null for unknown (which does not match). Steps lists every comparison on the way.</summary>
    public bool? Evaluate(RoutingMessage message, List<SqlFilterStep>? steps = null)
    {
        var value = _root.Evaluate(new Context(message, steps));
        return value switch
        {
            null => null,
            bool result => result,
            _ => throw new SqlFilterNotSupportedException("The filter is a value, not a condition.")
        };
    }

    private sealed record Context(RoutingMessage Message, List<SqlFilterStep>? Steps);

    private abstract class Node
    {
        public abstract object? Evaluate(Context context);
    }

    private sealed class Constant(object? value) : Node
    {
        public object? Value { get; } = value;

        public override object? Evaluate(Context context) => Value;
    }

    private sealed class Property(string scope, string name) : Node
    {
        public string Display => scope == "user" ? name : $"{scope}.{name}";

        /// <summary>
        /// Numbers are widened to long or double for comparison. Guid, date, time span, character, URI and binary
        /// values are left to Service Bus: how SQL compares them with literals is not something to guess.
        /// </summary>
        public override object? Evaluate(Context context)
        {
            var value = RoutingValue.Normalize(Resolve(context, out _));
            return value is null or string or bool or long or double
                ? value
                : throw new SqlFilterNotSupportedException(
                    $"{Display} is a {value.GetType().Name}; QueueLoom leaves comparisons on such values to Service Bus.");
        }

        public object? Resolve(Context context, out bool exists) =>
            scope == "sys" ? context.Message.System(name, out exists) : context.Message.User(name, out exists);
    }

    private sealed class Unary(string op, Node operand) : Node
    {
        public override object? Evaluate(Context context)
        {
            var value = operand.Evaluate(context);
            return (op, value) switch
            {
                ("NOT", null) => null,
                ("NOT", bool flag) => !flag,
                ("-", long number) => number == long.MinValue ? throw Overflow() : -number,
                ("-", double number) => -number,
                ("+", long or double) => value,
                (_, null) => null,
                _ => null
            };
        }
    }

    private sealed class Logical(string op, Node left, Node right) : Node
    {
        public override object? Evaluate(Context context)
        {
            var a = left.Evaluate(context) as bool?;
            var b = right.Evaluate(context) as bool?;
            return op == "AND"
                ? a == false || b == false ? false : a == true && b == true ? true : null
                : a == true || b == true ? true : a == false && b == false ? false : null;
        }
    }

    /// <summary>A whole-number result outside Int64: how Service Bus treats it is not something to guess.</summary>
    private static SqlFilterNotSupportedException Overflow() =>
        new("The arithmetic goes beyond a 64-bit whole number; Service Bus decides what that gives.");

    private sealed class Arithmetic(string op, Node left, Node right) : Node
    {
        public override object? Evaluate(Context context)
        {
            var a = left.Evaluate(context);
            var b = right.Evaluate(context);
            if (a is long x && b is long y)
            {
                try
                {
                    return op switch
                    {
                        "+" => checked(x + y),
                        "-" => checked(x - y),
                        "*" => checked(x * y),
                        "/" => y == 0 ? null : x == long.MinValue && y == -1 ? throw Overflow() : x / y,
                        _ => y == 0 ? null : x == long.MinValue && y == -1 ? throw Overflow() : x % y
                    };
                }
                catch (OverflowException)
                {
                    throw Overflow();
                }
            }
            if (Number(a) is { } p && Number(b) is { } q)
            {
                return op switch
                {
                    "+" => p + q,
                    "-" => p - q,
                    "*" => p * q,
                    "/" => q == 0 ? null : p / q,
                    _ => q == 0 ? null : p % q
                };
            }
            if (op == "+" && a is string s && b is string t)
            {
                return s + t;
            }
            return null;
        }
    }

    /// <summary>A comparison, LIKE, IN, IS NULL or EXISTS: the leaves that explain a result.</summary>
    private abstract class Test(string text) : Node
    {
        public override object? Evaluate(Context context)
        {
            var result = Check(context, out var actual);
            context.Steps?.Add(new SqlFilterStep(text, result, actual));
            return result;
        }

        protected abstract bool? Check(Context context, out string? actual);

        protected static string? Describe(Node node, Context context)
        {
            if (node is not Property property)
            {
                return null;
            }
            var value = property.Resolve(context, out var exists);
            return exists ? $"{property.Display} is {Format(value)}" : $"the message has no {property.Display}";
        }
    }

    private sealed class Comparison(string text, string op, Node left, Node right) : Test(text)
    {
        protected override bool? Check(Context context, out string? actual)
        {
            actual = Describe(left, context) ?? Describe(right, context);
            var order = Compare(left.Evaluate(context), right.Evaluate(context), op is "=" or "<>" or "!=");
            return order is not { } value
                ? null
                : op switch
                {
                    "=" => value == 0,
                    "<>" or "!=" => value != 0,
                    "<" => value < 0,
                    ">" => value > 0,
                    "<=" => value <= 0,
                    _ => value >= 0
                };
        }
    }

    private sealed class Like(string text, Node value, string pattern, char? escape, bool negated) : Test(text)
    {
        // Each element is a literal character, '_' (one character) or '%' (any run), with escapes applied.
        private readonly (char Character, char Kind)[] _pattern = Compile(pattern, escape);

        protected override bool? Check(Context context, out string? actual)
        {
            actual = Describe(value, context);
            return value.Evaluate(context) is string candidate ? Matches(candidate) != negated : null;
        }

        private static (char, char)[] Compile(string pattern, char? escape)
        {
            var result = new List<(char, char)>();
            for (var index = 0; index < pattern.Length; index++)
            {
                var character = pattern[index];
                if (escape is { } escapeCharacter && character == escapeCharacter && index + 1 < pattern.Length)
                {
                    result.Add((pattern[++index], 'c'));
                }
                else
                {
                    result.Add((character, character switch { '%' => '%', '_' => '_', _ => 'c' }));
                }
            }
            return [.. result];
        }

        /// <summary>
        /// Wildcard matching that backtracks only to the last '%', so it takes at most pattern × text steps; a
        /// regular expression of ".*" runs can take exponential time on patterns such as '%a%a%a%a%b'.
        /// </summary>
        private bool Matches(string candidate)
        {
            int text = 0, position = 0, star = -1, resume = 0;
            while (text < candidate.Length)
            {
                if (position < _pattern.Length && _pattern[position].Kind == '%')
                {
                    star = position++;
                    resume = text;
                }
                else if (position < _pattern.Length &&
                         (_pattern[position].Kind == '_' || _pattern[position].Character == candidate[text]))
                {
                    position++;
                    text++;
                }
                else if (star >= 0)
                {
                    position = star + 1;
                    text = ++resume;
                }
                else
                {
                    return false;
                }
            }
            while (position < _pattern.Length && _pattern[position].Kind == '%')
            {
                position++;
            }
            return position == _pattern.Length;
        }
    }

    private sealed class In(string text, Node value, IReadOnlyList<Node> options, bool negated) : Test(text)
    {
        protected override bool? Check(Context context, out string? actual)
        {
            actual = Describe(value, context);
            var candidate = value.Evaluate(context);
            if (candidate is null)
            {
                return null;
            }
            var found = options.Any(option => Compare(candidate, option.Evaluate(context), equality: true) == 0);
            return found != negated;
        }
    }

    private sealed class IsNull(string text, Node value, bool negated) : Test(text)
    {
        protected override bool? Check(Context context, out string? actual)
        {
            actual = Describe(value, context);
            // A null test needs no comparison, so a property of any type (Guid, date…) is decided here.
            var resolved = value is Property property ? property.Resolve(context, out _) : value.Evaluate(context);
            return resolved is null != negated;
        }
    }

    private sealed class Exists(string text, Property property) : Test(text)
    {
        protected override bool? Check(Context context, out string? actual)
        {
            property.Resolve(context, out var exists);
            actual = exists ? null : $"the message has no {property.Display}";
            return exists;
        }
    }

    /// <summary>-1, 0 or 1; null when either side is missing or the two cannot be compared.</summary>
    private static int? Compare(object? a, object? b, bool equality)
    {
        switch (a, b)
        {
            case (null, _) or (_, null):
                return null;
            case (string x, string y):
                return Math.Sign(string.CompareOrdinal(x, y));
            case (bool x, bool y) when equality:
                return x == y ? 0 : 1;
            case (long x, long y):
                return x.CompareTo(y);
        }
        return Number(a) is { } p && Number(b) is { } q ? p.CompareTo(q) : null;
    }

    private static double? Number(object? value) => value switch
    {
        long number => number,
        double number => number,
        _ => null
    };

    private static string Format(object? value) => value switch
    {
        null => "null",
        string text => $"'{text}'",
        bool flag => flag ? "TRUE" : "FALSE",
        IFormattable number => number.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "null"
    };

    private enum TokenKind
    {
        Identifier,
        Bracketed,
        String,
        Number,
        Symbol,
        End
    }

    private readonly record struct Token(TokenKind Kind, string Text, int Start, int End);

    private sealed class Parser
    {
        private readonly string _text;
        private readonly List<Token> _tokens;
        private int _index;

        public Parser(string text)
        {
            _text = text;
            _tokens = Tokenize(text);
        }

        public Node ParseAll()
        {
            if (Peek.Kind == TokenKind.End)
            {
                throw new SqlFilterSyntaxException("The filter is empty.", 0);
            }
            var node = ParseOr();
            if (Peek.Kind != TokenKind.End)
            {
                throw Error($"Unexpected '{Peek.Text}'.");
            }
            return node;
        }

        private Token Peek => _tokens[_index];

        private Token Next() => _tokens[_index++];

        private bool IsKeyword(string keyword, int offset = 0) =>
            _tokens[Math.Min(_index + offset, _tokens.Count - 1)] is { Kind: TokenKind.Identifier } token &&
            string.Equals(token.Text, keyword, StringComparison.OrdinalIgnoreCase);

        private bool IsSymbol(string symbol) => Peek.Kind == TokenKind.Symbol && Peek.Text == symbol;

        private SqlFilterSyntaxException Error(string message) => new(message, Peek.Start);

        private void Expect(string symbol)
        {
            if (!IsSymbol(symbol))
            {
                throw Error($"Expected '{symbol}'" + (Peek.Kind == TokenKind.End ? " at the end." : $" before '{Peek.Text}'."));
            }
            _index++;
        }

        private Node ParseOr()
        {
            var node = ParseAnd();
            while (IsKeyword("OR"))
            {
                _index++;
                node = new Logical("OR", node, ParseAnd());
            }
            return node;
        }

        private Node ParseAnd()
        {
            var node = ParseNot();
            while (IsKeyword("AND"))
            {
                _index++;
                node = new Logical("AND", node, ParseNot());
            }
            return node;
        }

        private Node ParseNot()
        {
            if (IsKeyword("NOT"))
            {
                _index++;
                return new Unary("NOT", ParseNot());
            }
            return ParseTest();
        }

        private Node ParseTest()
        {
            var start = Peek.Start;
            if (IsKeyword("EXISTS"))
            {
                _index++;
                Expect("(");
                var property = ParseAdditive() as Property ?? throw Error("EXISTS takes a property name.");
                Expect(")");
                return new Exists(Slice(start), property);
            }

            var left = ParseAdditive();
            if (Peek.Kind == TokenKind.Symbol && Peek.Text is "=" or "<>" or "!=" or "<" or ">" or "<=" or ">=")
            {
                var op = Next().Text;
                var right = ParseAdditive();
                return new Comparison(Slice(start), op, left, right);
            }

            var negated = false;
            if (IsKeyword("NOT") && (IsKeyword("LIKE", 1) || IsKeyword("IN", 1)))
            {
                _index++;
                negated = true;
            }
            if (IsKeyword("LIKE"))
            {
                _index++;
                if (Peek.Kind != TokenKind.String)
                {
                    throw Error("LIKE takes a quoted pattern, for example 'order.%'.");
                }
                var pattern = Next().Text;
                char? escape = null;
                if (IsKeyword("ESCAPE"))
                {
                    _index++;
                    if (Peek.Kind != TokenKind.String || Peek.Text.Length != 1)
                    {
                        throw Error("ESCAPE takes one quoted character, for example '!'.");
                    }
                    escape = Next().Text[0];
                }
                return new Like(Slice(start), left, pattern, escape, negated);
            }
            if (IsKeyword("IN"))
            {
                _index++;
                Expect("(");
                var options = new List<Node> { ParseAdditive() };
                while (IsSymbol(","))
                {
                    _index++;
                    options.Add(ParseAdditive());
                }
                Expect(")");
                return new In(Slice(start), left, options, negated);
            }
            if (IsKeyword("IS"))
            {
                _index++;
                var isNot = false;
                if (IsKeyword("NOT"))
                {
                    _index++;
                    isNot = true;
                }
                if (!IsKeyword("NULL"))
                {
                    throw Error("Expected NULL after IS.");
                }
                _index++;
                return new IsNull(Slice(start), left, isNot);
            }
            return left;
        }

        private Node ParseAdditive()
        {
            var node = ParseMultiplicative();
            while (Peek.Kind == TokenKind.Symbol && Peek.Text is "+" or "-")
            {
                node = new Arithmetic(Next().Text, node, ParseMultiplicative());
            }
            return node;
        }

        private Node ParseMultiplicative()
        {
            var node = ParseUnary();
            while (Peek.Kind == TokenKind.Symbol && Peek.Text is "*" or "/" or "%")
            {
                node = new Arithmetic(Next().Text, node, ParseUnary());
            }
            return node;
        }

        private Node ParseUnary()
        {
            if (Peek.Kind == TokenKind.Symbol && Peek.Text is "-" or "+")
            {
                return new Unary(Next().Text, ParseUnary());
            }
            return ParsePrimary();
        }

        private Node ParsePrimary()
        {
            var token = Peek;
            switch (token.Kind)
            {
                case TokenKind.Symbol when token.Text == "(":
                    _index++;
                    var inner = ParseOr();
                    Expect(")");
                    return inner;
                case TokenKind.String:
                    _index++;
                    return new Constant(token.Text);
                case TokenKind.Number:
                    _index++;
                    return new Constant(ParseNumber(token));
                case TokenKind.Bracketed:
                    _index++;
                    return new Property("user", token.Text);
                case TokenKind.Identifier:
                    _index++;
                    var upper = token.Text.ToUpperInvariant();
                    switch (upper)
                    {
                        case "TRUE":
                            return new Constant(true);
                        case "FALSE":
                            return new Constant(false);
                        case "NULL":
                            return new Constant(null);
                    }
                    if (IsSymbol("("))
                    {
                        throw new SqlFilterNotSupportedException($"QueueLoom cannot evaluate the function {token.Text}(); Service Bus can.");
                    }
                    if (IsSymbol(".") && upper is "SYS" or "USER")
                    {
                        _index++;
                        var name = Peek.Kind is TokenKind.Identifier or TokenKind.Bracketed
                            ? Next().Text
                            : throw Error($"Expected a property name after {token.Text}.");
                        return new Property(upper.ToLowerInvariant(), name);
                    }
                    return new Property("user", token.Text);
                case TokenKind.End:
                    throw Error("The filter ends too early.");
                default:
                    throw Error($"Unexpected '{token.Text}'.");
            }
        }

        private object ParseNumber(Token token)
        {
            if (long.TryParse(token.Text, NumberStyles.None, CultureInfo.InvariantCulture, out var integer))
            {
                return integer;
            }
            if (double.TryParse(token.Text, NumberStyles.Float, CultureInfo.InvariantCulture, out var real))
            {
                return real;
            }
            throw new SqlFilterSyntaxException($"'{token.Text}' is not a number.", token.Start);
        }

        private string Slice(int start) => _text[start.._tokens[_index - 1].End].Trim();

        private static List<Token> Tokenize(string text)
        {
            var tokens = new List<Token>();
            var index = 0;
            while (index < text.Length)
            {
                var character = text[index];
                if (char.IsWhiteSpace(character))
                {
                    index++;
                    continue;
                }
                var start = index;
                if (character == '\'')
                {
                    var value = new StringBuilder();
                    index++;
                    while (true)
                    {
                        if (index >= text.Length)
                        {
                            throw new SqlFilterSyntaxException("A quoted string is not closed.", start);
                        }
                        if (text[index] == '\'')
                        {
                            if (index + 1 < text.Length && text[index + 1] == '\'')
                            {
                                value.Append('\'');
                                index += 2;
                                continue;
                            }
                            index++;
                            break;
                        }
                        value.Append(text[index++]);
                    }
                    tokens.Add(new Token(TokenKind.String, value.ToString(), start, index));
                }
                else if (character == '[')
                {
                    var close = text.IndexOf(']', index + 1);
                    if (close < 0)
                    {
                        throw new SqlFilterSyntaxException("A [bracketed] name is not closed.", start);
                    }
                    tokens.Add(new Token(TokenKind.Bracketed, text[(index + 1)..close], start, close + 1));
                    index = close + 1;
                }
                else if (char.IsAsciiDigit(character) || character == '.' && index + 1 < text.Length && char.IsAsciiDigit(text[index + 1]))
                {
                    while (index < text.Length && (char.IsAsciiDigit(text[index]) || text[index] == '.'))
                    {
                        index++;
                    }
                    if (index < text.Length && text[index] is 'e' or 'E')
                    {
                        index++;
                        if (index < text.Length && text[index] is '+' or '-')
                        {
                            index++;
                        }
                        while (index < text.Length && char.IsAsciiDigit(text[index]))
                        {
                            index++;
                        }
                    }
                    tokens.Add(new Token(TokenKind.Number, text[start..index], start, index));
                }
                else if (char.IsLetter(character) || character == '_' || character == '$')
                {
                    while (index < text.Length && (char.IsLetterOrDigit(text[index]) || text[index] is '_' or '$'))
                    {
                        index++;
                    }
                    tokens.Add(new Token(TokenKind.Identifier, text[start..index], start, index));
                }
                else
                {
                    var two = index + 1 < text.Length ? text.Substring(index, 2) : string.Empty;
                    var symbol = two is "<>" or "!=" or "<=" or ">=" ? two : character.ToString();
                    if (symbol.Length == 1 && "=<>()+-*/%,.".IndexOf(character) < 0)
                    {
                        throw new SqlFilterSyntaxException($"Unexpected character '{character}'.", start);
                    }
                    index += symbol.Length;
                    tokens.Add(new Token(TokenKind.Symbol, symbol, start, index));
                }
            }
            tokens.Add(new Token(TokenKind.End, string.Empty, text.Length, text.Length));
            return tokens;
        }
    }
}
