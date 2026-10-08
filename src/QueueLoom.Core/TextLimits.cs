namespace QueueLoom.Core;

/// <summary>Cutting text to a length without breaking a character in two.</summary>
public static class TextLimits
{
    /// <summary>
    /// The first <paramref name="maximum"/> UTF-16 characters of <paramref name="text"/>, one fewer when the cut would
    /// fall between the two halves of a character outside the Basic Multilingual Plane (an emoji, for example). A lone
    /// half shows as a broken glyph and is invalid in JSON for strict readers.
    /// </summary>
    public static string Head(string text, int maximum)
    {
        ArgumentNullException.ThrowIfNull(text);
        ArgumentOutOfRangeException.ThrowIfNegative(maximum);
        if (text.Length <= maximum) return text;
        var length = maximum > 0 && char.IsHighSurrogate(text[maximum - 1]) ? maximum - 1 : maximum;
        return text[..length];
    }
}
