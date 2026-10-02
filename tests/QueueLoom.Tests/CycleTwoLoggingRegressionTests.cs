using System.Reflection;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using QueueLoom.Core.Diagnostics;
using QueueLoom.Infrastructure.Logging;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

[CollectionDefinition("CycleTwo redactor injection", DisableParallelization = true)]
public sealed class CycleTwoRedactorCollection;

// The production regex is restored before another collection can run. No CPU-load or timing dependency.
[Collection("CycleTwo redactor injection")]
public sealed class CycleTwoLoggingRegressionTests
{
    [Fact]
    public void CycleTwoLogging_RedactorTimeoutOmitsSensitiveInputAndRecovers()
    {
        WithTimeout(() =>
        {
            var result = SensitiveDataRedactor.Redact("password=PRIVATE_SENTINEL");
            Assert.False(string.IsNullOrWhiteSpace(result));
            Assert.DoesNotContain("PRIVATE_SENTINEL", result, StringComparison.Ordinal);
            var summary = SensitiveDataRedactor.SummarizeException(new IOException("password=PRIVATE_SENTINEL"));
            Assert.DoesNotContain("PRIVATE_SENTINEL", summary, StringComparison.Ordinal);
        });
        Assert.Equal("password=[REDACTED]", SensitiveDataRedactor.Redact("password=PRIVATE_SENTINEL"));
    }

    [Fact]
    public void CycleTwoLogging_FileLoggerContainsActualRedactionTimeoutAndThenWritesAgain()
    {
        using var directory = new TemporaryDirectory();
        using var provider = new FileLoggerProvider(directory.Path);
        var logger = provider.CreateLogger("isolated");
        WithTimeout(() => logger.LogError(new IOException("password=PRIVATE_SENTINEL"), "password=PRIVATE_SENTINEL"));
        logger.LogInformation("recovered");
        var text = File.ReadAllText(provider.CurrentFilePath);
        Assert.Contains("recovered", text, StringComparison.Ordinal);
        Assert.DoesNotContain("PRIVATE_SENTINEL", text, StringComparison.Ordinal);
    }

    [Fact]
    public void CycleTwoLogging_FormatterFailureCannotInterruptWorkOrWriteUnredactedFallback()
    {
        using var directory = new TemporaryDirectory();
        using var provider = new FileLoggerProvider(directory.Path);
        var logger = provider.CreateLogger("isolated");
        logger.Log(LogLevel.Error, default, "password=PRIVATE_SENTINEL", null,
            (_, _) => throw new InvalidOperationException("password=PRIVATE_SENTINEL"));
        logger.LogInformation("recovered");
        var text = File.ReadAllText(provider.CurrentFilePath);
        Assert.Contains("recovered", text, StringComparison.Ordinal);
        Assert.DoesNotContain("PRIVATE_SENTINEL", text, StringComparison.Ordinal);
    }

    private static void WithTimeout(Action action)
    {
        var regex = (Regex)typeof(SensitiveDataRedactor).GetField("SensitiveValuePattern", BindingFlags.Static | BindingFlags.NonPublic)!.GetValue(null)!;
        var factory = typeof(Regex).GetField("factory", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var runner = typeof(Regex).GetField("_runner", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var priorFactory = factory.GetValue(regex); var priorRunner = runner.GetValue(regex);
        try { factory.SetValue(regex, new TimeoutFactory()); runner.SetValue(regex, null); action(); }
        finally { factory.SetValue(regex, priorFactory); runner.SetValue(regex, priorRunner); }
    }
    private sealed class TimeoutFactory : RegexRunnerFactory
    {
        protected override RegexRunner CreateInstance() => new TimeoutRunner();
    }
    private sealed class TimeoutRunner : RegexRunner
    {
        protected override void Scan(ReadOnlySpan<char> text) => throw new RegexMatchTimeoutException();
    }
}
