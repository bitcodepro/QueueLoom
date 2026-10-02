using System.Text;
using System.Text.Json;
using System.Diagnostics;
using QueueLoom.Core.Monitoring;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class CycleOneHistoryRegressionTests
{
    private static readonly DateTimeOffset Now = DateTimeOffset.Parse("2026-10-02T12:00:00Z");
    private static readonly Guid Profile = Guid.NewGuid();
    private static DeadLetterHistorySample Sample(DateTimeOffset at, long total) => new(at, Profile, "Test", total, new Dictionary<string, long> { ["q"] = total });
    private sealed class Clock : TimeProvider { public override DateTimeOffset GetUtcNow() => Now; }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task IndependentProcessesCannotCompactAwayAnAcknowledgedSample(bool overlap)
    {
        using var directory = new TemporaryDirectory();
        var file = Path.Combine(directory.Path, "history.jsonl");
        File.WriteAllText(file, JsonSerializer.Serialize(Sample(DateTimeOffset.UtcNow.AddDays(-40), 1)) + "\n");
        var fixture = Path.Combine(AppContext.BaseDirectory, "UpdateFixture", "QueueLoom.UpdateFixture" + (OperatingSystem.IsWindows() ? ".exe" : string.Empty));
        var first = Path.Combine(directory.Path, "first");
        var second = Path.Combine(directory.Path, "second");
        using var a = Start(first, "3");
        using var b = Start(second, "2");
        try
        {
            var deadline = Stopwatch.StartNew();
            while (!File.Exists(first + ".ready") || !File.Exists(second + ".ready"))
            {
                Assert.False(a.HasExited || b.HasExited, "A history worker exited before loading the shared snapshot.");
                Assert.True(deadline.Elapsed < TimeSpan.FromSeconds(15));
                await Task.Delay(20);
            }
            if (overlap)
            {
                using (var ownership = new FileStream(file + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None))
                {
                    File.WriteAllText(first + ".go", "go");
                    File.WriteAllText(second + ".go", "go");
                    deadline.Restart();
                    while (!File.Exists(first + ".started") || !File.Exists(second + ".started"))
                    {
                        Assert.True(deadline.Elapsed < TimeSpan.FromSeconds(15));
                        await Task.Delay(20);
                    }
                    await Task.Delay(200);
                    Assert.False(File.Exists(first + ".done") || File.Exists(second + ".done"), "Writers must wait for the shared ownership lock.");
                }
            }
            else
            {
                File.WriteAllText(second + ".go", "go");
                await b.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(15));
                Assert.Equal(0, b.ExitCode);
                File.WriteAllText(first + ".go", "go");
            }
            await b.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(15));
            Assert.Equal(0, b.ExitCode);
            await a.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(15));
            Assert.Equal(0, a.ExitCode);
            Assert.True(File.Exists(first + ".done") && File.Exists(second + ".done"));
            Assert.Equal([2L, 3L], new JsonLinesDeadLetterHistoryStore(file).Read(Profile, DateTimeOffset.MinValue).Select(s => s.Total).Order());
        }
        finally
        {
            foreach (var child in new[] { a, b }) if (!child.HasExited) { child.Kill(); await child.WaitForExitAsync(); }
        }
        Process Start(string output, string total)
        {
            var start = new ProcessStartInfo(fixture) { UseShellExecute = false, WindowStyle = ProcessWindowStyle.Hidden };
            foreach (var argument in new[] { "--append-history", file, Profile.ToString(), output, output + ".go", total }) start.ArgumentList.Add(argument);
            return Process.Start(start)!;
        }
    }

    [Fact]
    public void FailedAppendDoesNotCacheAnUncommittedSample()
    {
        if (!OperatingSystem.IsWindows()) return; // Windows sharing rules deterministically reject the attempted write.
        using var directory = new TemporaryDirectory();
        var file = Path.Combine(directory.Path, "history.jsonl");
        File.WriteAllText(file, JsonSerializer.Serialize(Sample(Now.AddMinutes(-2), 1)) + "\n");
        var store = new JsonLinesDeadLetterHistoryStore(file, new Clock());
        Assert.Single(store.Read(Profile, DateTimeOffset.MinValue));
        using (var blocked = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            Assert.Throws<IOException>(() => store.Append(Sample(Now, 2)));
        }
        Assert.Equal([1L], store.Read(Profile, DateTimeOffset.MinValue).Select(s => s.Total));
        store.Append(Sample(Now, 2));
        Assert.Equal([1L, 2L], new JsonLinesDeadLetterHistoryStore(file, new Clock()).Read(Profile, DateTimeOffset.MinValue).Select(s => s.Total));
    }

    [Theory]
    [InlineData("{\"At\":", false)]
    [InlineData("{\"At\": broken\n", true)]
    [InlineData("", true)]
    [InlineData("valid-final-record", false)]
    public void AppendAfterCrashTailRetainsEveryAcknowledgedSample(string tail, bool terminated)
    {
        using var directory = new TemporaryDirectory();
        var file = Path.Combine(directory.Path, "history.jsonl");
        var first = Sample(Now.AddMinutes(-3), 1);
        var extra = tail == "valid-final-record" ? JsonSerializer.Serialize(Sample(Now.AddMinutes(-2), 2)) : tail;
        var prefix = JsonSerializer.Serialize(first) + "\n" + extra;
        File.WriteAllText(file, prefix, new UTF8Encoding(false));
        var store = new JsonLinesDeadLetterHistoryStore(file, new Clock());
        store.Read(Profile, Now.AddDays(-1));
        store.Append(Sample(Now.AddMinutes(-1), 3));
        var reopened = new JsonLinesDeadLetterHistoryStore(file, new Clock());
        Assert.Equal(tail == "valid-final-record" ? new long[] { 1, 2, 3 } : [1L, 3L], reopened.Read(Profile, Now.AddDays(-1)).Select(s => s.Total));
        Assert.StartsWith(prefix, File.ReadAllText(file), StringComparison.Ordinal);
        if (!terminated && extra.Length > 0) Assert.Contains(extra + "\n", File.ReadAllText(file), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void StaleWriterCannotCompactAwayAnotherWritersAcknowledgedSample(bool expiredSeed)
    {
        using var directory = new TemporaryDirectory();
        var file = Path.Combine(directory.Path, "history.jsonl");
        var seed = Sample(expiredSeed ? Now.AddDays(-40) : Now.AddMinutes(-5), 1);
        File.WriteAllText(file, JsonSerializer.Serialize(seed) + "\n");
        var first = new JsonLinesDeadLetterHistoryStore(file, new Clock());
        var second = new JsonLinesDeadLetterHistoryStore(file, new Clock());
        first.Read(Profile, DateTimeOffset.MinValue);
        second.Read(Profile, DateTimeOffset.MinValue);
        second.Append(Sample(Now.AddMinutes(-2), 2));
        first.Append(Sample(Now.AddMinutes(-1), 3));
        var reopened = new JsonLinesDeadLetterHistoryStore(file, new Clock());
        Assert.Equal(expiredSeed ? new long[] { 2, 3 } : [1L, 2L, 3L], reopened.Read(Profile, DateTimeOffset.MinValue).Select(s => s.Total));
        Assert.Equal([2L, 3L], second.Read(Profile, Now.AddMinutes(-3)).Select(s => s.Total));
    }
}
