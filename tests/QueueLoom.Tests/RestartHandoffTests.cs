using System.Diagnostics;
using QueueLoom.Core.Updates;

namespace QueueLoom.Tests;

public sealed class RestartHandoffTests
{
    [Fact]
    public async Task AParentThatOutlivesTheWaitDelaysTheRestartInsteadOfFailingIt()
    {
        // The test runner itself is a parent that will not exit within the wait.
        using var self = Process.GetCurrentProcess();
        var exited = await RestartHandoff.WaitForParentAsync(self.Id, self.StartTime.ToUniversalTime().Ticks, TimeSpan.FromMilliseconds(100));
        Assert.False(exited);
    }

    [Fact]
    public async Task AParentThatIsGoneEndsTheWaitAtOnce()
    {
        using var child = Process.Start(new ProcessStartInfo("dotnet", "--version") { UseShellExecute = false, RedirectStandardOutput = true })!;
        var id = child.Id;
        var started = child.StartTime.ToUniversalTime().Ticks;
        await child.WaitForExitAsync();
        Assert.True(await RestartHandoff.WaitForParentAsync(id, started, TimeSpan.FromSeconds(10)));
    }
}
