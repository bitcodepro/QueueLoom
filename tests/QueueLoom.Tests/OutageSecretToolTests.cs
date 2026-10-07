using System.Diagnostics;
using System.Reflection;
using QueueLoom.Infrastructure.Security;

namespace QueueLoom.Tests;

[Collection("Fake secret-tool")]
public sealed class OutageSecretToolTests
{
    [Fact]
    public async Task Outage_CancelledSecretToolCannotFinishAfterTheNextStore()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Portable Windows fixture; Linux backend has its own gated test."); return; }
        var directory = PrepareFixture(badMarker: false);
        await RunIsolatedAsync(directory, async (store, cancellation) =>
        {
            var pending = store("fake-key-A", cancellation.Token);
            using var first = Process.GetProcessById(await ReadFirstPidAsync(directory));
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
            // Exercise the race all the way through, rather than only checking a disposal flag.
            await store("fake-key-B", default);
            File.WriteAllText(Path.Combine(directory, "release-first-store"), "release");
            if (!first.HasExited) await first.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("fake-key-B", File.ReadAllText(Path.Combine(directory, "stored")));
        });
    }

    // The scenario fails before it learns the child's PID (here: an unreadable marker). Its own failure is what is
    // reported, and the child it started is still cancelled and joined, so the directory can be removed. Before, the
    // child kept running and deleting the directory threw UnauthorizedAccessException, hiding the real failure.
    [Fact]
    public async Task Outage_AFailureBeforeThePidIsKnownStillStopsTheChild()
    {
        if (!OperatingSystem.IsWindows()) { Assert.Skip("Portable Windows fixture; Linux backend has its own gated test."); return; }
        var directory = PrepareFixture(badMarker: true);

        await Assert.ThrowsAsync<FormatException>(() => RunIsolatedAsync(directory, async (store, cancellation) =>
        {
            _ = store("fake-key-A", cancellation.Token);
            _ = await ReadFirstPidAsync(directory);
        }));

        Assert.Empty(FixtureProcesses(directory));
        Assert.False(Directory.Exists(directory), "The fixture directory could not be removed.");
    }

    private static string PrepareFixture(bool badMarker)
    {
        var directory = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var fixture = Path.Combine(AppContext.BaseDirectory, "UpdateFixture");
        foreach (var file in Directory.EnumerateFiles(fixture)) File.Copy(file, Path.Combine(directory, Path.GetFileName(file)));
        File.Copy(Path.Combine(fixture, "QueueLoom.UpdateFixture.exe"), Path.Combine(directory, "secret-tool.exe"));
        if (badMarker) File.WriteAllText(Path.Combine(directory, "bad-marker"), "1");
        return directory;
    }

    /// <summary>
    /// Runs a scenario with the fake secret-tool first on PATH. Whatever happens, every store it started is cancelled
    /// and awaited (the production runner kills and joins its child), any fixture process still alive is killed and
    /// joined, and only then is the directory removed. A cleanup failure never replaces the scenario's own exception;
    /// if cleanup alone fails, it names the processes that still hold the directory.
    /// </summary>
    private static async Task RunIsolatedAsync(string directory,
        Func<Func<string, CancellationToken, Task>, CancellationTokenSource, Task> scenario)
    {
        var oldPath = Environment.GetEnvironmentVariable("PATH");
        Environment.SetEnvironmentVariable("PATH", directory + Path.PathSeparator + oldPath);
        using var cancellation = new CancellationTokenSource();
        var method = typeof(LinuxSecretServiceMasterKeyStore).GetMethod("RunSecretToolAsync", BindingFlags.Static | BindingFlags.NonPublic)!;
        var started = new List<Task>();
        Task Store(string value, CancellationToken token)
        {
            var task = (Task)method.Invoke(null, [new[] { "store" }, value, token])!;
            lock (started) started.Add(task);
            return task;
        }
        var failed = false;
        try
        {
            await scenario(Store, cancellation);
        }
        catch
        {
            failed = true;
            throw;
        }
        finally
        {
            Environment.SetEnvironmentVariable("PATH", oldPath);
            try
            {
                await CleanUpAsync(directory, cancellation, started);
            }
            catch (Exception) when (failed)
            {
                // The scenario's failure is the one to report.
            }
        }
    }

    private static async Task CleanUpAsync(string directory, CancellationTokenSource cancellation, List<Task> started)
    {
        cancellation.Cancel();
        Task[] stores;
        lock (started) stores = [.. started];
        foreach (var store in stores)
        {
            try { await store.WaitAsync(TimeSpan.FromSeconds(30)); }
            catch (Exception) { /* Cancelled or failed: either way it has stopped, which is all cleanup needs. */ }
        }
        foreach (var process in FixtureProcesses(directory))
        {
            using (process)
            {
                if (!process.HasExited) process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync().WaitAsync(TimeSpan.FromSeconds(10));
            }
        }
        var left = FixtureProcesses(directory);
        if (left.Count > 0)
        {
            var names = string.Join(", ", left.Select(process => $"{process.ProcessName} ({process.Id})"));
            left.ForEach(process => process.Dispose());
            throw new InvalidOperationException("Fixture processes still hold the directory: " + names);
        }
        Directory.Delete(directory, recursive: true);
    }

    /// <summary>Running processes whose executable is in the fixture directory (the fake secret-tool).</summary>
    private static List<Process> FixtureProcesses(string directory)
    {
        var root = Path.GetFullPath(directory) + Path.DirectorySeparatorChar;
        var found = new List<Process>();
        foreach (var process in Process.GetProcessesByName("secret-tool"))
        {
            try
            {
                if (process.MainModule?.FileName is { } file && file.StartsWith(root, StringComparison.OrdinalIgnoreCase))
                {
                    found.Add(process);
                    continue;
                }
            }
            catch (Exception exception) when (exception is InvalidOperationException or System.ComponentModel.Win32Exception)
            {
                // Exited meanwhile, or not ours to inspect.
            }
            process.Dispose();
        }
        return found;
    }

    private static async Task<int> ReadFirstPidAsync(string directory)
    {
        var marker = Path.Combine(directory, "first-store.pid");
        for (var i = 0; i < 200 && !File.Exists(marker); i++) await Task.Delay(10);
        Assert.True(File.Exists(marker), "The isolated child did not reach the barrier.");
        // The fixture publishes the marker by rename, so it is complete once it exists.
        return int.Parse(File.ReadAllText(marker), System.Globalization.CultureInfo.InvariantCulture);
    }
}
