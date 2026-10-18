using System.ComponentModel;
using System.Diagnostics;
using QueueLoom.Core.Updates;

namespace QueueLoom.Tests;

// The restart launcher waited at most two minutes for the closing application and then failed: an application slow to
// close (a large backup) closed, and no new version started. On Linux and macOS it did not check the process identity
// either, so a process that reused the PID was waited for. It now waits up to half an hour and checks an identity
// taken while the application is known to be alive: the kernel creation time on Windows, the boot-relative start
// ticks on Linux (wall-clock changes do not move them).
public sealed class LauncherRestartWaitTests
{
    private static readonly TimeSpan Short = TimeSpan.FromMilliseconds(50);

    [Fact]
    public void TheWaitIsLongEnoughForASlowClose() =>
        Assert.True(ApplicationExitWait.DefaultLimit >= TimeSpan.FromMinutes(30));

    [Theory]
    [InlineData(new long[] { 5, 5, -2 }, ApplicationExitWait.Result.Exited)]        // -2: gone
    [InlineData(new long[] { 5, 6 }, ApplicationExitWait.Result.Exited)]            // the PID now belongs to another process
    [InlineData(new long[] { 5 }, ApplicationExitWait.Result.StillRunning)]         // still the application at the limit
    public async Task TheWaitEndsWhenTheIdentityIsGone(long[] readings, ApplicationExitWait.Result expected)
    {
        var index = 0;
        long? Probe(int _) { var value = readings[Math.Min(index++, readings.Length - 1)]; return value == -2 ? null : value; }

        var watch = new ApplicationExitWait.Watch(Environment.ProcessId, 5, Probe, TimeProvider.System);

        Assert.Equal(expected, await watch.WaitAsync(expected == ApplicationExitWait.Result.StillRunning ? Short : TimeSpan.FromSeconds(10)));
    }

    // The process exits between the lookup and the identity read: the read fails, and the exit is confirmed rather than
    // assumed. A process that is present but unreadable is still waited for.
    [Fact]
    public void AFailedReadCountsAsExitOnlyWhenTheProcessIsGone()
    {
        static long? Unreadable(int _) => throw new Win32Exception("process information disappeared");
        using var finished = FinishedProcessId(out var gone);

        Assert.Null(ApplicationExitWait.Inspect(Unreadable, gone, known: 5));
        Assert.Equal(5, ApplicationExitWait.Inspect(Unreadable, Environment.ProcessId, known: 5));
        Assert.Equal(-1, ApplicationExitWait.Inspect(Unreadable, Environment.ProcessId, known: null));
    }

    // A live application whose wall-clock start time reads differently (the clock was corrected) is still waited for on
    // Linux; on Windows the creation time is a kernel value and a different one means another process.
    [Fact]
    public async Task AWallClockDifferenceDoesNotEndTheWaitOnLinux()
    {
        using var self = Process.GetCurrentProcess();
        var shifted = self.StartTime.ToUniversalTime().AddMinutes(-5).Ticks;

        var result = await ApplicationExitWait.Identify(self.Id, shifted).WaitAsync(Short);

        Assert.Equal(OperatingSystem.IsWindows() ? ApplicationExitWait.Result.Exited : ApplicationExitWait.Result.StillRunning, result);
    }

    [Fact]
    public void LinuxStartTicksAreStable()
    {
        if (!OperatingSystem.IsLinux()) return;

        var first = ApplicationExitWait.LinuxStartTicks(Environment.ProcessId);

        Assert.NotNull(first);
        Assert.Equal(first, ApplicationExitWait.LinuxStartTicks(Environment.ProcessId));
    }

    // A real application, identified while it is held alive, then released: the wait sees it gone.
    [Fact]
    public async Task AReleasedApplicationIsSeenToExit()
    {
        var root = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "exit-wait " + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var lockFile = Path.Combine(root, "app.lock");
        using var application = StartFixture("--hold-lock", lockFile, "until-released");
        try
        {
            await WaitForFile(lockFile + ".held");
            var watch = ApplicationExitWait.Identify(application.Id, application.StartTime.ToUniversalTime().Ticks);
            Assert.Equal(ApplicationExitWait.Result.StillRunning, await watch.WaitAsync(Short));

            await File.WriteAllTextAsync(lockFile + ".release", "go");

            Assert.Equal(ApplicationExitWait.Result.Exited, await watch.WaitAsync(TimeSpan.FromSeconds(15)));
            Assert.True(application.WaitForExit(15_000), "The released fixture did not exit.");
            Assert.Equal(0, application.ExitCode);
        }
        finally
        {
            if (!application.HasExited) { application.Kill(); application.WaitForExit(15_000); }
            try { Directory.Delete(root, recursive: true); } catch (IOException) { }
        }
    }

    private static string Fixture => Path.Combine(AppContext.BaseDirectory, "UpdateFixture", "QueueLoom.UpdateFixture" + (OperatingSystem.IsWindows() ? ".exe" : ""));

    private static Process StartFixture(params string[] arguments)
    {
        var start = new ProcessStartInfo(Fixture) { UseShellExecute = false };
        foreach (var argument in arguments) start.ArgumentList.Add(argument);
        return Process.Start(start)!;
    }

    /// <summary>A process started, held until released, then observed to have exited; its PID is no longer in use.</summary>
    private static IDisposable FinishedProcessId(out int id)
    {
        var root = Path.Combine(Path.GetTempPath(), "QueueLoom.Tests", "finished " + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        var lockFile = Path.Combine(root, "app.lock");
        var process = StartFixture("--hold-lock", lockFile, "until-released");
        WaitForFile(lockFile + ".held").GetAwaiter().GetResult();
        id = process.Id;
        File.WriteAllText(lockFile + ".release", "go");
        Assert.True(process.WaitForExit(15_000), "The released fixture did not exit.");
        process.Dispose();
        return new Cleanup(root);
    }

    private sealed class Cleanup(string root) : IDisposable
    {
        public void Dispose() { try { Directory.Delete(root, recursive: true); } catch (IOException) { } }
    }

    private static async Task WaitForFile(string path)
    {
        var watch = Stopwatch.StartNew();
        while (!File.Exists(path))
        {
            if (watch.Elapsed > TimeSpan.FromSeconds(15)) throw new TimeoutException(path);
            await Task.Delay(25);
        }
    }
}
