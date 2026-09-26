using QueueLoom.Core.Diagnostics;

namespace QueueLoom.Tests;

public sealed class SensitiveDataRedactorTests
{
    [Theory]
    [InlineData("Endpoint=sb://ns/;SharedAccessKeyName=root;SharedAccessKey=abc=", "SharedAccessKey=[REDACTED]")]
    [InlineData("https://ns/?sig=AbC%2B&se=1", "sig=[REDACTED]")]
    [InlineData("client_secret = hunter2", "client_secret=[REDACTED]")]
    public void Redact_RemovesCredentialValues(string input, string expected)
    {
        var redacted = SensitiveDataRedactor.Redact(input);

        Assert.Contains(expected, redacted, StringComparison.Ordinal);
        Assert.DoesNotContain("hunter2", redacted, StringComparison.Ordinal);
        Assert.DoesNotContain("abc=", redacted, StringComparison.Ordinal);
    }

    [Fact]
    public void SummarizeException_IsSingleLineBoundedAndUsesTheBaseException()
    {
        var inner = new InvalidOperationException("line one\r\nSharedAccessKey=secret " + new string('x', 2_000));
        var summary = SensitiveDataRedactor.SummarizeException(new AggregateException(inner));

        Assert.DoesNotContain('\n', summary);
        Assert.StartsWith("line one", summary, StringComparison.Ordinal);
        Assert.Contains("SharedAccessKey=[REDACTED]", summary, StringComparison.Ordinal);
        Assert.True(summary.Length <= 601);
    }
}
