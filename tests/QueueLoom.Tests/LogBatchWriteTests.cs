using Microsoft.Extensions.Logging;
using QueueLoom.Infrastructure.Logging;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// The writer took the cross-process lock and reopened the log once per line. With the desktop app and an MCP server
// writing the same daily log, that capped throughput at a few hundred lines a second, and shutdown could give up with
// lines still queued. Everything queued is now appended as one bounded batch per lock hold.
public sealed class LogBatchWriteTests
{
    private static async Task<(FileLoggerProvider Provider, Func<int> Acquisitions, ManualResetEventSlim Gate)> StartGated(
        string directory, Func<DateTimeOffset> clock)
    {
        System.IO.Directory.CreateDirectory(directory);
        var acquisitions = 0;
        var gate = new ManualResetEventSlim(false);
        FileLoggerProvider.AcquireWriteLockOverride.Value = path =>
        {
            Interlocked.Increment(ref acquisitions);
            gate.Wait();
            return new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 1);
        };
        try
        {
            var provider = new FileLoggerProvider(directory, clock: clock);
            await Task.Yield();
            return (provider, () => Volatile.Read(ref acquisitions), gate);
        }
        finally
        {
            // The writer thread captured the override when it started; later tests must not see it.
            FileLoggerProvider.AcquireWriteLockOverride.Value = null;
        }
    }

    [Fact]
    public async Task QueuedLinesShareOneLockHoldPerBatch()
    {
        using var directory = new TemporaryDirectory();
        var now = new DateTimeOffset(2026, 3, 1, 12, 0, 0, TimeSpan.Zero);
        var (provider, acquisitions, gate) = await StartGated(directory.Path, () => now);
        using (provider)
        {
            var logger = provider.CreateLogger("app");
            const int count = 2000;
            for (var i = 0; i < count; i++)
            {
                logger.LogInformation("line {Index}", i);
            }
            gate.Set();
            provider.Flush();

            var lines = File.ReadAllLines(provider.CurrentFilePath);
            Assert.Equal(Enumerable.Range(0, count).Select(i => $"line {i}"),
                lines.Select(line => line[(line.IndexOf("app: ", StringComparison.Ordinal) + 5)..]));
            // The first line may be taken alone while the rest are still being queued; the others go in full batches.
            Assert.InRange(acquisitions(), 1, 1 + (int)Math.Ceiling(count / (double)FileLoggerProvider.MaxBatchLines));
        }
    }

    [Fact]
    public async Task ABatchSpanningMidnightGoesToEachLinesOwnDay()
    {
        using var directory = new TemporaryDirectory();
        var now = new DateTimeOffset(2026, 3, 1, 23, 59, 59, TimeSpan.Zero);
        var (provider, _, gate) = await StartGated(directory.Path, () => now);
        using (provider)
        {
            var logger = provider.CreateLogger("app");
            logger.LogInformation("before 1");
            logger.LogInformation("before 2");
            now = now.AddSeconds(2);
            logger.LogInformation("after 1");
            logger.LogInformation("after 2");
            gate.Set();
            provider.Flush();

            var first = File.ReadAllText(Path.Combine(directory.Path, "queueloom-20260301.log"));
            var second = File.ReadAllText(Path.Combine(directory.Path, "queueloom-20260302.log"));
            Assert.True(first.IndexOf("before 1", StringComparison.Ordinal) < first.IndexOf("before 2", StringComparison.Ordinal));
            Assert.DoesNotContain("after", first, StringComparison.Ordinal);
            Assert.True(second.IndexOf("after 1", StringComparison.Ordinal) < second.IndexOf("after 2", StringComparison.Ordinal));
            Assert.DoesNotContain("before", second, StringComparison.Ordinal);
        }
    }
}
