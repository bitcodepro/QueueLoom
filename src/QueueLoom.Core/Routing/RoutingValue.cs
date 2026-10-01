using System.Globalization;

namespace QueueLoom.Core.Routing;

/// <summary>
/// Property values as rules see them: text, whole numbers, decimals and booleans keep their type, so 250 and '250'
/// stay different, as they are in Service Bus. Written like SQL literals: 'text', 250, 1.5, true.
/// </summary>
public static class RoutingValue
{
    /// <summary>Whole numbers become long and fractional ones double, so values from any source compare alike.</summary>
    public static object? Normalize(object? value) => value switch
    {
        byte or sbyte or short or ushort or int or uint or long => Convert.ToInt64(value, CultureInfo.InvariantCulture),
        ulong number when number <= long.MaxValue => (long)number,
        float or double or decimal or ulong => Convert.ToDouble(value, CultureInfo.InvariantCulture),
        _ => value
    };

    /// <summary>Equality as Service Bus applies it: numbers by value, text exactly (case-sensitive), never text against a number.</summary>
    public static bool AreEqual(object? left, object? right)
    {
        var a = Normalize(left);
        var b = Normalize(right);
        return (a, b) switch
        {
            (null, _) or (_, null) => false,
            (string x, string y) => string.Equals(x, y, StringComparison.Ordinal),
            (bool x, bool y) => x == y,
            (long x, long y) => x == y,
            (long or double, long or double) => Convert.ToDouble(a, CultureInfo.InvariantCulture) == Convert.ToDouble(b, CultureInfo.InvariantCulture),
            _ => Equals(a, b)
        };
    }

    public static string Format(object? value) => Normalize(value) switch
    {
        null => "null",
        string text => $"'{text.Replace("'", "''", StringComparison.Ordinal)}'",
        bool flag => flag ? "true" : "false",
        long number => number.ToString(CultureInfo.InvariantCulture),
        double number => number.ToString("R", CultureInfo.InvariantCulture),
        // Types the editor cannot write (a Guid, a date) are shown as they are and kept as they are.
        var other => $"<{other.GetType().Name}> {Convert.ToString(other, CultureInfo.InvariantCulture)}"
    };

    /// <summary>'quoted' is text, true and false are booleans, 250 and 1.5 are numbers; anything else is text as written.</summary>
    public static object Parse(string text)
    {
        var value = text.Trim();
        if (value.Length >= 2 && value[0] == '\'' && value[^1] == '\'')
        {
            return value[1..^1].Replace("''", "'", StringComparison.Ordinal);
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
}
