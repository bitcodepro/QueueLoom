using System.Text;

namespace QueueLoom.App.ViewModels;

/// <summary>How a session's state reads in Explorer: its text when it is UTF-8, otherwise its bytes.</summary>
public static class SessionStateDisplay
{
    private const int MaximumBytesShown = 4096;
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public static string Describe(byte[]? state)
    {
        if (state is not { Length: > 0 })
        {
            return "This session has no state.";
        }
        try
        {
            var text = StrictUtf8.GetString(state);
            if (!text.Any(character => char.IsControl(character) && character is not ('\r' or '\n' or '\t')))
            {
                return text.Length > MaximumBytesShown ? text[..MaximumBytesShown] + "…" : text;
            }
        }
        catch (DecoderFallbackException)
        {
            // Not text: shown as bytes below.
        }
        var shown = Convert.ToHexString(state, 0, Math.Min(state.Length, MaximumBytesShown / 3));
        var pairs = string.Join(' ', Enumerable.Range(0, shown.Length / 2).Select(index => shown.Substring(index * 2, 2)));
        return $"{state.Length:N0} bytes, not text: {pairs}{(state.Length > MaximumBytesShown / 3 ? " …" : string.Empty)}";
    }
}
