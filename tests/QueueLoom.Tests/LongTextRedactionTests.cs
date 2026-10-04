using QueueLoom.Core.Diagnostics;

namespace QueueLoom.Tests;

public sealed class LongTextRedactionTests
{
    /// <summary>
    /// A secret at the end of a long, word-rich text (a large stack trace written to the log) is redacted. With the
    /// NonBacktracking engine it was left as is, and with a 100 ms timeout the whole text could be omitted instead.
    /// </summary>
    [Theory]
    [InlineData(2_000)]
    [InlineData(60_000)]
    public void LongTextIsRedactedNotLeftOrOmitted(int frames)
    {
        var text = string.Concat(Enumerable.Repeat("   at QueueLoom.Some.Frame.Method(String argument) in /src/file.cs:line 42\n", frames))
            + "password=hunter2";
        var redacted = SensitiveDataRedactor.Redact(text);
        Assert.DoesNotContain("Text omitted", redacted, StringComparison.Ordinal);
        Assert.DoesNotContain("hunter2", redacted, StringComparison.Ordinal);
        Assert.EndsWith("password=[REDACTED]", redacted, StringComparison.Ordinal);
    }
}
