using Microsoft.Extensions.Logging;
using QueueLoom.Infrastructure.Logging;

namespace QueueLoom.Tests.Infrastructure;

public sealed class FileLoggerProviderTests
{
    private static readonly DateTimeOffset Now = new(2026, 9, 26, 10, 0, 0, TimeSpan.Zero);

    [Fact]
    public void Log_WritesRedactedLinesToTheDailyFile()
    {
        using var directory = new TemporaryDirectory();
        using var provider = new FileLoggerProvider(directory.Path, clock: () => Now);
        var logger = provider.CreateLogger("QueueLoom.Test");

        logger.LogError(
            new InvalidOperationException("Endpoint=sb://x/;SharedAccessKey=abc123"),
            "Connect failed for {Connection}",
            "SharedAccessKeyName=root;SharedAccessKey=secret-value");
        provider.Flush();

        var text = File.ReadAllText(provider.CurrentFilePath);
        Assert.EndsWith("queueloom-20260926.log", provider.CurrentFilePath, StringComparison.Ordinal);
        Assert.Contains("[ERR] QueueLoom.Test: Connect failed", text, StringComparison.Ordinal);
        Assert.Contains("SharedAccessKey=[REDACTED]", text, StringComparison.Ordinal);
        Assert.DoesNotContain("secret-value", text, StringComparison.Ordinal);
        Assert.DoesNotContain("abc123", text, StringComparison.Ordinal);
    }

    [Fact]
    public void Log_BelowMinimumLevelIsIgnored()
    {
        using var directory = new TemporaryDirectory();
        using var provider = new FileLoggerProvider(directory.Path, LogLevel.Warning, clock: () => Now);

        provider.CreateLogger("QueueLoom.Test").LogInformation("routine");
        provider.Flush();

        Assert.False(File.Exists(provider.CurrentFilePath));
    }

    [Fact]
    public void Constructor_DeletesLogsOlderThanTheRetentionWindow()
    {
        using var directory = new TemporaryDirectory();
        var expired = Path.Combine(directory.Path, "queueloom-20260801.log");
        var recent = Path.Combine(directory.Path, "queueloom-20260925.log");
        File.WriteAllText(expired, "old");
        File.WriteAllText(recent, "new");

        using var provider = new FileLoggerProvider(directory.Path, retainedDays: 14, clock: () => Now);

        Assert.False(File.Exists(expired));
        Assert.True(File.Exists(recent));
    }

    [Fact]
    public void Log_AfterDisposeDoesNotThrowOrWrite()
    {
        using var directory = new TemporaryDirectory();
        var provider = new FileLoggerProvider(directory.Path, clock: () => Now);
        var logger = provider.CreateLogger("QueueLoom.Test");
        provider.Dispose();

        logger.LogError("late message");

        Assert.False(File.Exists(provider.CurrentFilePath));
    }

    [Fact]
    public void Log_QueuedBeforeDisposeIsFlushed()
    {
        using var directory = new TemporaryDirectory();
        var provider = new FileLoggerProvider(directory.Path, clock: () => Now);
        provider.CreateLogger("QueueLoom.Test").LogInformation("before dispose");
        var path = provider.CurrentFilePath;
        provider.Dispose();

        Assert.Contains("before dispose", File.ReadAllText(path), StringComparison.Ordinal);
    }
}
