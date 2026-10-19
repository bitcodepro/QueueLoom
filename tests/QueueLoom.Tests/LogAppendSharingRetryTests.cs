using Microsoft.Extensions.Logging;
using QueueLoom.Infrastructure.Logging;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// The cross-process lock serializes QueueLoom's own appends, but another program (an antivirus scan, a log viewer
// that opens the file exclusively) can hold the daily log for a moment. The append then failed with a sharing
// violation and the line was dropped at once. On a busy machine this lost lines from a burst, which is also why
// Logging_ConcurrentProcessesWritingTheSameDailyLogAccountForEveryLine failed now and then. A sharing violation is now
// retried briefly before the line counts as dropped.
public sealed class LogAppendSharingRetryTests
{
    [Fact]
    public async Task ALineIsWrittenOnceABriefExclusiveHolderLetsGo()
    {
        using var directory = new TemporaryDirectory();
        var now = new DateTimeOffset(2026, 3, 1, 12, 0, 0, TimeSpan.Zero);
        using var provider = new FileLoggerProvider(directory.Path, clock: () => now);
        var logger = provider.CreateLogger("desktop");
        logger.LogInformation("first");
        provider.Flush();

        var holder = new FileStream(provider.CurrentFilePath, FileMode.Open, FileAccess.Read, FileShare.None);
        logger.LogInformation("while held");
        var release = Task.Delay(300).ContinueWith(_ => holder.Dispose(), TaskScheduler.Default);
        await release;
        provider.Flush();

        var text = File.ReadAllText(provider.CurrentFilePath);
        Assert.Contains("while held", text, StringComparison.Ordinal);
        Assert.DoesNotContain("log lines dropped", text, StringComparison.Ordinal);
    }
}
