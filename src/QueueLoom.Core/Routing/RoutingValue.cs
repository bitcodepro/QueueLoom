using System.Globalization;
using System.Text;

namespace QueueLoom.Core.Routing;

/// <summary>
/// Property values as rules see them. Every value keeps its type, so 250 and '250' stay different, as they are in
/// Service Bus. Written one per line: 'text' (with \n for a line break), 250 (Int64), 1.5 (Double), true, and a type
/// tag for the rest, for example &lt;Int32&gt; 7, &lt;Guid&gt; 4f3c2a1b-… or &lt;Single&gt; 0.1.
/// </summary>
public static class RoutingValue
{
    /// <summary>Whole numbers become long and fractional ones double, for the SQL filter evaluator.</summary>
    public static object? Normalize(object? value) => value switch
    {
        byte or sbyte or short or ushort or int or uint or long => Convert.ToInt64(value, CultureInfo.InvariantCulture),
        ulong number when number <= long.MaxValue => (long)number,
        // A float is widened through its shortest text, so 0.1f becomes 0.1 and not 0.10000000149011612.
        float number => double.Parse(number.ToString("R", CultureInfo.InvariantCulture), CultureInfo.InvariantCulture),
        double or decimal or ulong => Convert.ToDouble(value, CultureInfo.InvariantCulture),
        _ => value
    };

    /// <summary>Whether QueueLoom can compare this value faithfully (the types Service Bus accepts in correlation filters).</summary>
    public static bool IsComparable(object? value) => value is null or string or bool or char or Guid or DateTime or DateTimeOffset or TimeSpan
        or Uri or byte or sbyte or short or ushort or int or uint or long or ulong or float or double or decimal;

    /// <summary>The types Service Bus accepts in a correlation filter it is given through its administration API.</summary>
    public static bool IsAllowedInCorrelationRule(object? value) => value is string or int or long or bool or double or DateTime;

    /// <summary>
    /// Equality as a correlation filter applies it, which the Service Bus emulator confirms: the type must match as
    /// well as the value, so the Int32 7 does not match the Int64 7 and 250 never matches '250'. Text is compared
    /// exactly (case-sensitive). SQL filters are different: they compare numbers by value.
    /// </summary>
    public static bool AreEqual(object? left, object? right)
    {
        if (left is null || right is null || left.GetType() != right.GetType())
        {
            return false;
        }
        return (left, right) switch
        {
            (string x, string y) => string.Equals(x, y, StringComparison.Ordinal),
            (Uri x, Uri y) => string.Equals(x.OriginalString, y.OriginalString, StringComparison.Ordinal),
            (byte[] x, byte[] y) => x.AsSpan().SequenceEqual(y),
            _ => left.Equals(right)
        };
    }

    public static string Format(object? value) => value switch
    {
        null => "null",
        string text => Quote(text),
        bool flag => flag ? "true" : "false",
        long number => number.ToString(CultureInfo.InvariantCulture),
        // Other whole-number types are tagged, because a correlation filter tells Int32 7 from Int64 7.
        byte or sbyte or short or ushort or int or uint or ulong => $"<{value.GetType().Name}> {Convert.ToString(value, CultureInfo.InvariantCulture)}",
        double number => DoubleText(number),
        float number => $"<Single> {number.ToString("R", CultureInfo.InvariantCulture)}",
        decimal number => $"<Decimal> {number.ToString(CultureInfo.InvariantCulture)}",
        char character => $"<Char> {Quote(character.ToString())}",
        Guid guid => $"<Guid> {guid:D}",
        DateTime time => $"<DateTime> {time.ToString("O", CultureInfo.InvariantCulture)}",
        DateTimeOffset time => $"<DateTimeOffset> {time.ToString("O", CultureInfo.InvariantCulture)}",
        TimeSpan span => $"<TimeSpan> {span.ToString("c", CultureInfo.InvariantCulture)}",
        Uri uri => $"<Uri> {uri.OriginalString}",
        _ => $"<{value.GetType().Name}> {Convert.ToString(value, CultureInfo.InvariantCulture)}"
    };

    /// <summary>
    /// Reads what <see cref="Format"/> writes. Unquoted text that is not a number or boolean is kept as text, so
    /// "acme" works as well as "'acme'".
    /// </summary>
    public static object Parse(string text)
    {
        var value = text.Trim();
        if (value.StartsWith('<') && value.IndexOf('>') is > 1 and var close)
        {
            var type = value[1..close];
            var rest = value[(close + 1)..].Trim();
            try
            {
                return type.ToUpperInvariant() switch
                {
                    "SINGLE" => float.Parse(rest, NumberStyles.Float, CultureInfo.InvariantCulture),
                    "DECIMAL" => decimal.Parse(rest, NumberStyles.Number, CultureInfo.InvariantCulture),
                    "CHAR" => Unquote(rest) is { Length: 1 } single ? single[0] : throw new FormatException("A <Char> holds exactly one character."),
                    "GUID" => Guid.Parse(rest),
                    "DATETIME" => DateTime.Parse(rest, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
                    "DATETIMEOFFSET" => DateTimeOffset.Parse(rest, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
                    "TIMESPAN" => TimeSpan.Parse(rest, CultureInfo.InvariantCulture),
                    "URI" => new Uri(rest, UriKind.RelativeOrAbsolute),
                    "BYTE" => byte.Parse(rest, CultureInfo.InvariantCulture),
                    "SBYTE" => sbyte.Parse(rest, CultureInfo.InvariantCulture),
                    "INT16" => short.Parse(rest, CultureInfo.InvariantCulture),
                    "UINT16" => ushort.Parse(rest, CultureInfo.InvariantCulture),
                    "INT32" => int.Parse(rest, CultureInfo.InvariantCulture),
                    "UINT32" => uint.Parse(rest, CultureInfo.InvariantCulture),
                    "INT64" => long.Parse(rest, CultureInfo.InvariantCulture),
                    "UINT64" => ulong.Parse(rest, CultureInfo.InvariantCulture),
                    "DOUBLE" => double.Parse(rest, NumberStyles.Float, CultureInfo.InvariantCulture),
                    "STRING" => Unquote(rest),
                    _ => throw new UnknownTypeException(type)
                };
            }
            catch (Exception exception) when (exception is OverflowException or UriFormatException or ArgumentException or FormatException
                                                  && exception is not UnknownTypeException)
            {
                throw new FormatException(exception is OverflowException
                    ? $"{rest} does not fit in a <{type}>."
                    : $"{(rest.StartsWith('\'') ? rest : $"'{rest}'")} is not a valid <{type}>.", exception);
            }
        }
        if (value.Length >= 2 && value[0] == '\'' && value[^1] == '\'')
        {
            return Unquote(value);
        }
        if (value.Equals("true", StringComparison.OrdinalIgnoreCase) || value.Equals("false", StringComparison.OrdinalIgnoreCase))
        {
            return value.Equals("true", StringComparison.OrdinalIgnoreCase);
        }
        if (long.TryParse(value, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var integer))
        {
            return integer;
        }
        if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var real))
        {
            return real;
        }
        return value;
    }

    /// <summary>"250.0" rather than "250", so a decimal stays a decimal when the line is read back.</summary>
    private static string DoubleText(double number)
    {
        var text = number.ToString("R", CultureInfo.InvariantCulture);
        return double.IsFinite(number) && !text.Contains('.') && !text.Contains('E') ? text + ".0" : text;
    }

    private sealed class UnknownTypeException(string type) : FormatException(
        $"Unknown type <{type}>. Use Int32, Int64, Double, Single, Decimal, Char, Guid, DateTime, DateTimeOffset, TimeSpan or Uri.");

    private static bool IsNumber(object value) =>
        value is byte or sbyte or short or ushort or int or uint or long or ulong or float or double or decimal;

    /// <summary>'text' with '' for a quote and \n, \r, \t, \\ for line breaks, tabs and backslashes.</summary>
    private static string Quote(string text)
    {
        var builder = new StringBuilder("'");
        foreach (var character in text)
        {
            builder.Append(character switch
            {
                '\'' => "''",
                '\\' => "\\\\",
                '\n' => "\\n",
                '\r' => "\\r",
                '\t' => "\\t",
                _ => character.ToString()
            });
        }
        return builder.Append('\'').ToString();
    }

    private static string Unquote(string quoted)
    {
        var inner = quoted.Length >= 2 && quoted[0] == '\'' && quoted[^1] == '\'' ? quoted[1..^1] : quoted;
        var builder = new StringBuilder();
        for (var index = 0; index < inner.Length; index++)
        {
            var character = inner[index];
            if (character == '\'' && index + 1 < inner.Length && inner[index + 1] == '\'')
            {
                builder.Append('\'');
                index++;
            }
            else if (character == '\\' && index + 1 < inner.Length && inner[index + 1] is 'n' or 'r' or 't' or '\\')
            {
                builder.Append(inner[++index] switch { 'n' => '\n', 'r' => '\r', 't' => '\t', _ => '\\' });
            }
            else
            {
                builder.Append(character);
            }
        }
        return builder.ToString();
    }
}
