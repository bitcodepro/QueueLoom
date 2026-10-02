using System.Diagnostics;
using QueueLoom.Core.Settings;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed class SettingsTransactionRegressionTests
{
    [Fact]
    public async Task TwoProcesses_PreserveBothIndependentSettingsUpdates()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        using var store = new JsonAppSettingsStore(paths);
        await store.SaveMonitorIntervalSecondsAsync(60);
        var first = Path.Combine(directory.Path, "first");
        var second = Path.Combine(directory.Path, "second");
        var release = Path.Combine(directory.Path, "release");
        using var a = Start("theme", first);
        Process? b = null;
        try
        {
            await WaitFor(first + ".read", a); // A has read the old document and cannot commit until released.
            b = Start("interval", second);
            await WaitFor(second + ".started", b);
            // Give B a chance to finish while A owns its transaction. A correct store blocks B until A commits.
            await Task.WhenAny(b.WaitForExitAsync(), Task.Delay(TimeSpan.FromSeconds(2)));
            await File.WriteAllTextAsync(release, "release");
            await Task.WhenAll(a.WaitForExitAsync(), b.WaitForExitAsync()).WaitAsync(TimeSpan.FromSeconds(15));
            Assert.Equal(0, a.ExitCode);
            Assert.Equal(0, b.ExitCode);
            var settings = await store.LoadAsync();
            Assert.Equal(321, settings.MonitorIntervalSeconds);
            Assert.Equal(AppThemePreference.Light, settings.Theme);
            Assert.Equal("Light", await File.ReadAllTextAsync(second + ".read"));
        }
        finally
        {
            foreach (var process in new[] { a, b })
                if (process is { HasExited: false }) { process.Kill(); await process.WaitForExitAsync(); }
            b?.Dispose();
        }

        Process Start(string operation, string output)
        {
            var fixture = Path.Combine(AppContext.BaseDirectory, "UpdateFixture", "QueueLoom.UpdateFixture" +
                (OperatingSystem.IsWindows() ? ".exe" : string.Empty));
            var start = new ProcessStartInfo(fixture) { UseShellExecute = false, WindowStyle = ProcessWindowStyle.Hidden };
            foreach (var argument in new[] { "--update-settings", directory.Path, operation, output, release }) start.ArgumentList.Add(argument);
            return Process.Start(start)!;
        }
    }

    [Fact]
    public async Task ContendingStore_CancellationDoesNotRunTheUpdateOrOverwriteSettings()
    {
        using var directory = new TemporaryDirectory();
        var paths = QueueLoomPaths.ForRoot(directory.Path);
        using var a = new JsonAppSettingsStore(paths);
        using var b = new JsonAppSettingsStore(paths);
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var first = Task.Run(() => a.UpdateAsync(settings =>
        {
            entered.Set();
            if (!release.Wait(TimeSpan.FromSeconds(15))) throw new TimeoutException("Transaction barrier timed out.");
            return settings with { Theme = AppThemePreference.Light };
        }));
        Assert.True(entered.Wait(TimeSpan.FromSeconds(15)));
        var callbacks = 0;
        try
        {
            using var cancel = new CancellationTokenSource(TimeSpan.FromMilliseconds(250));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => b.UpdateAsync(settings =>
            {
                Interlocked.Increment(ref callbacks);
                return settings with { MonitorIntervalSeconds = 777 };
            }, cancel.Token));
            Assert.Equal(0, callbacks);
        }
        finally { release.Set(); await first; }
        var saved = await b.LoadAsync();
        Assert.Equal(AppThemePreference.Light, saved.Theme);
        Assert.Equal(60, saved.MonitorIntervalSeconds);
    }

    private static async Task WaitFor(string path, Process process)
    {
        var clock = Stopwatch.StartNew();
        while (!File.Exists(path))
        {
            Assert.False(process.HasExited, $"Settings process exited before {Path.GetFileName(path)}.");
            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(15), "Settings process barrier timed out.");
            await Task.Delay(10);
        }
    }
}
