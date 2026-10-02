using System.Text.RegularExpressions;

namespace QueueLoom.Core.Diagnostics;

/// <summary>Removes credential material from text that is shown to operators or written to logs.</summary>
public static class SensitiveDataRedactor
{
    private static readonly Regex SensitiveValuePattern = new(
        @"\b(SharedAccessKey|SharedAccessSignature|sig|password|client_secret)\s*=\s*[^;\s&]+",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.NonBacktracking,
        TimeSpan.FromMilliseconds(100));

    public static string Redact(string? text)
    {
        if (string.IsNullOrEmpty(text)) return string.Empty;
        try { return SensitiveValuePattern.Replace(text, "$1=[REDACTED]"); }
        catch (RegexMatchTimeoutException)
        {
            // Neither partial replacement nor raw input is safe to publish.
            return "[Text omitted: credential redaction timed out]";
        }
    }

    /// <summary>A single-line, bounded, redacted summary of an exception for display.</summary>
    public static string SummarizeException(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        var raw = exception.GetBaseException().Message;
        if (raw.Length > 5_000)
        {
            raw = raw[..5_000];
        }
        var text = Redact(raw)
            .Replace('\r', ' ')
            .Replace('\n', ' ')
            .Trim();
        return text.Length > 600 ? text[..600] + "…" : text;
    }
}
