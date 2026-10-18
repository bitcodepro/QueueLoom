using System.Diagnostics;
using QueueLoom.Core.Updates;

namespace QueueLoom.Tests;

// The restart launcher waited at most two minutes for the closing application and then failed: an application slow to
// close (a large backup) closed, and no new version started. On Linux and macOS it did not check the process start time
// either, so a process that reused the PID was waited for. It now waits up to half an hour, says so when that runs out,
// and checks the start time on every system (within a second, as other systems round it).
public sealed class LauncherRestartWaitTests
{
    [Fact]
    public void TheWaitIsLongEnoughForASlowClose() =>
        Assert.True(ApplicationExitWait.DefaultLimit >= TimeSpan.FromMinutes(30));

    [Fact]
    public async Task AProcessThatNoLongerExistsHasExited()
    {
        using var finished = Process.Start(new ProcessStartInfo("dotnet", "--version") { RedirectStandardOutput = true, UseShellExecute = false })!;
        var id = finished.Id;
        var start = finished.StartTime.ToUniversalTime().Ticks;
        await finished.WaitForExitAsync();
        finished.Dispose();

        Assert.Equal(ApplicationExitWait.Result.Exited, await ApplicationExitWait.WaitAsync(id, start, TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task AProcessThatReusedThePidIsNotWaitedFor()
    {
        using var self = Process.GetCurrentProcess();
        var otherStart = self.StartTime.ToUniversalTime().AddMinutes(-5).Ticks;

        Assert.Equal(ApplicationExitWait.Result.Exited, await ApplicationExitWait.WaitAsync(self.Id, otherStart, TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task AnApplicationStillRunningAtTheLimitIsReportedNotThrown()
    {
        using var self = Process.GetCurrentProcess();
        var start = self.StartTime.ToUniversalTime().Ticks;

        Assert.Equal(ApplicationExitWait.Result.StillRunning, await ApplicationExitWait.WaitAsync(self.Id, start, TimeSpan.FromMilliseconds(50)));
    }

    // Another system rounds the start time differently in the two processes; a sub-second difference is the same process.
    [Fact]
    public async Task ASubSecondStartTimeDifferenceIsTheSameProcess()
    {
        using var self = Process.GetCurrentProcess();
        var start = self.StartTime.ToUniversalTime().AddMilliseconds(400).Ticks;

        Assert.Equal(ApplicationExitWait.Result.StillRunning, await ApplicationExitWait.WaitAsync(self.Id, start, TimeSpan.FromMilliseconds(50)));
    }
}
