using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using QueueLoom.Infrastructure.Logging;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

/// <summary>The background log writer: shutdown, flushing, failures and accounting (review of #87).</summary>
public sealed class FileLoggerLifecycleTests
{
    private static readonly DateTimeOffset Now = new(2026, 3, 1, 12, 0, 0, TimeSpan.Zero);

    private static FileStream HoldLock(string directory)
    {
        Directory.CreateDirectory(directory);
        return new FileStream(Path.Combine(directory, FileLoggerProvider.WriteLockName), FileMode.OpenOrCreate,
            FileAccess.ReadWrite, FileShare.None, 1);
    }

    private static int DroppedReported(string text) => text.Split('\n')
        .Where(line => line.Contains("log lines dropped because the log queue was full", StringComparison.Ordinal))
        .Sum(line => int.Parse(line[(line.IndexOf("] QueueLoom: ", StringComparison.Ordinal) + "] QueueLoom: ".Length)..].Split(' ')[0],
            System.Globalization.CultureInfo.InvariantCulture));

    // The desktop app's container owns the logger: disposing it at shutdown drains a line still waiting for the
    // cross-process lock, instead of losing it.
    [Fact]
    public async Task DisposingTheDesktopServicesDrainsQueuedLines()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        paths.EnsureCreated();
        var logs = Path.Combine(directory.Path, "logs");
        var services = QueueLoom.App.Services.AppServices.Build(paths);
        var logger = services.GetRequiredService<ILoggerFactory>().CreateLogger("shutdown");
        var held = HoldLock(logs);
        logger.LogWarning("last words");
        var release = Task.Run(async () =>
        {
            await Task.Delay(300);
            held.Dispose();
        });

        await services.DisposeAsync();
        await release;

        var file = Assert.Single(Directory.GetFiles(logs, "queueloom-*.log"));
        Assert.Contains("last words", File.ReadAllText(file), StringComparison.Ordinal);
    }

    // The lock is held for good by another process: Dispose returns within its one budget, the writer ends, and
    // nothing is written after Dispose returned, even once the lock is released.
    [Fact]
    public async Task ShutdownHasOneBudgetAndWritesNothingAfterIt()
    {
        using var directory = new TemporaryDirectory();
        var held = HoldLock(directory.Path);
        var provider = new FileLoggerProvider(directory.Path, clock: () => Now, shutdownBudget: TimeSpan.FromMilliseconds(300));
        var logger = provider.CreateLogger("app");
        for (var index = 0; index < 50; index++)
        {
            logger.LogInformation("queued {Index}", index);
        }

        var watch = Stopwatch.StartNew();
        provider.Dispose();
        watch.Stop();
        held.Dispose();
        await Task.Delay(300);

        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(2), $"Dispose took {watch.Elapsed}.");
        Assert.False(File.Exists(provider.CurrentFilePath), "A line was written after Dispose returned.");
    }

    // The lock is acquired only after the shutdown budget was spent and Dispose returned: that late acquisition must
    // not start an append.
    [Fact]
    public async Task ALockAcquiredAfterShutdownDoesNotWrite()
    {
        using var directory = new TemporaryDirectory();
        Directory.CreateDirectory(directory.Path);
        var entered = new SemaphoreSlim(0);
        var gate = new ManualResetEventSlim(false);
        FileLoggerProvider.AcquireWriteLockOverride.Value = path =>
        {
            entered.Release();
            gate.Wait();
            return new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 1);
        };
        FileLoggerProvider provider;
        try
        {
            provider = new FileLoggerProvider(directory.Path, clock: () => Now, shutdownBudget: TimeSpan.FromMilliseconds(200));
        }
        finally
        {
            // The writer thread captured the override when it started; later tests must not see it.
            FileLoggerProvider.AcquireWriteLockOverride.Value = null;
        }
        provider.CreateLogger("app").LogInformation("late line");
        Assert.True(await entered.WaitAsync(TimeSpan.FromSeconds(10)));

        provider.Dispose();
        gate.Set();
        await Task.Delay(300);

        Assert.False(File.Exists(provider.CurrentFilePath), "A line was written after Dispose returned.");
    }

    // The logs folder is renamed while the writer waits for the lock: the missing folder is not retried forever as
    // contention. The waiting line is reported as dropped, and later logging recreates the folder and goes on.
    [Fact]
    public void ARemovedLogFolderIsRecreated()
    {
        if (OperatingSystem.IsWindows()) return; // a folder with an open file cannot be renamed on Windows
        using var directory = new TemporaryDirectory();
        var logs = Path.Combine(directory.Path, "logs");
        var held = HoldLock(logs);
        var waiting = new ManualResetEventSlim(false);
        FileLoggerProvider.LockBusyObserved.Value = waiting.Set;
        FileLoggerProvider provider;
        try
        {
            // The writer thread captures the seam when it starts.
            provider = new FileLoggerProvider(logs, clock: () => Now);
        }
        finally
        {
            FileLoggerProvider.LockBusyObserved.Value = null;
        }
        using var owned = provider;
        var logger = provider.CreateLogger("app");
        logger.LogInformation("waiting");
        // The writer is in its retry loop on the held lock before the folder moves.
        Assert.True(waiting.Wait(TimeSpan.FromSeconds(10)));
        Directory.Move(logs, Path.Combine(directory.Path, "logs-old"));
        held.Dispose();

        logger.LogInformation("after the move");
        provider.Flush();

        var text = File.ReadAllText(provider.CurrentFilePath);
        Assert.Contains("after the move", text, StringComparison.Ordinal);
        Assert.Equal(1, DroppedReported(text));
    }

    // Flush waits for a full queue without holding anything other loggers need: logging from another thread goes on
    // at once (the line is counted as dropped), instead of waiting for the flush.
    [Fact]
    public async Task FlushDoesNotBlockOtherLoggers()
    {
        using var directory = new TemporaryDirectory();
        var held = HoldLock(directory.Path);
        using var provider = new FileLoggerProvider(directory.Path, clock: () => Now, maxQueuedLines: 2);
        var logger = provider.CreateLogger("app");
        for (var index = 0; index < 5; index++)
        {
            logger.LogInformation("filling {Index}", index);
        }
        var flush = Task.Run(provider.Flush);
        await Task.Delay(100);

        var watch = Stopwatch.StartNew();
        logger.LogWarning("while flushing");
        watch.Stop();
        held.Dispose();
        await flush.WaitAsync(TimeSpan.FromSeconds(20));

        Assert.True(watch.Elapsed < TimeSpan.FromMilliseconds(100), $"Logging waited {watch.Elapsed} for the flush.");
    }

    // An unexpected exception while writing one line costs that line only: the writer goes on, later lines are
    // written, and the lost line is reported.
    [Fact]
    public void AnUnexpectedWriteFailureDoesNotStopLogging()
    {
        using var directory = new TemporaryDirectory();
        var calls = 0;
        FileLoggerProvider.AcquireWriteLockOverride.Value = path =>
            ++calls == 1
                ? throw new InvalidOperationException("unexpected")
                : new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 1);
        try
        {
            using var provider = new FileLoggerProvider(directory.Path, clock: () => Now);
            var logger = provider.CreateLogger("app");
            logger.LogInformation("lost line");
            provider.Flush();
            logger.LogInformation("later line");
            provider.Flush();

            var text = File.ReadAllText(provider.CurrentFilePath);
            Assert.Contains("later line", text, StringComparison.Ordinal);
            Assert.DoesNotContain("lost line", text, StringComparison.Ordinal);
            Assert.Equal(1, DroppedReported(text));
        }
        finally
        {
            FileLoggerProvider.AcquireWriteLockOverride.Value = null;
        }
    }

    // A line logged just before midnight and written after it goes to the file of the day it was logged.
    [Fact]
    public void ALineGoesToTheFileOfTheDayItWasLogged()
    {
        using var directory = new TemporaryDirectory();
        var now = new DateTimeOffset(2026, 3, 1, 23, 59, 59, TimeSpan.Zero);
        var held = HoldLock(directory.Path);
        using var provider = new FileLoggerProvider(directory.Path, clock: () => now);
        provider.CreateLogger("app").LogInformation("before midnight");
        now = now.AddSeconds(2);
        held.Dispose();
        provider.Flush();

        Assert.Contains("before midnight", File.ReadAllText(Path.Combine(directory.Path, "queueloom-20260301.log")), StringComparison.Ordinal);
        Assert.False(File.Exists(Path.Combine(directory.Path, "queueloom-20260302.log")));
    }

    // Lines dropped while the queue was full are reported even when the first attempt to write the notice fails:
    // the count is kept until a notice is written, and written plus reported equals attempted.
    [Fact]
    public void DroppedLinesAreReportedAfterAFailedNotice()
    {
        using var directory = new TemporaryDirectory();
        var held = HoldLock(directory.Path);
        using var provider = new FileLoggerProvider(directory.Path, clock: () => Now, maxQueuedLines: 2);
        var logger = provider.CreateLogger("app");
        const int attempted = 10;
        for (var index = 0; index < attempted; index++)
        {
            logger.LogInformation("burst {Index}", index);
        }
        // The day's file is a directory for now: every append, the notice included, fails.
        Directory.CreateDirectory(provider.CurrentFilePath);
        held.Dispose();
        provider.Flush();
        Assert.True(provider.PendingOverflowCount > 0);

        Directory.Delete(provider.CurrentFilePath);
        logger.LogInformation("recovered");
        provider.Flush();

        var text = File.ReadAllText(provider.CurrentFilePath);
        var written = text.Split('\n').Count(line => line.Contains("[INF] app: burst ", StringComparison.Ordinal));
        Assert.Contains("recovered", text, StringComparison.Ordinal);
        Assert.Equal(attempted, written + DroppedReported(text));
        Assert.Equal(0, provider.PendingOverflowCount);
    }
}
