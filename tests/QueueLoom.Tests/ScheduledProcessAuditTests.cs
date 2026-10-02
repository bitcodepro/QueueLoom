using System.Diagnostics;
using QueueLoom.Core.Profiles;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task ScheduledClaimAcrossTwoProcessesHasExactlyOneWinner()
    {
        using var directory = new TemporaryDirectory();
        var root = directory.Path;
        var store = new JsonScheduledResendStore(QueueLoomPaths.ForRoot(root));
        store.Save([DueJob(CreateProfile("Processes", EnvironmentKind.Development, ProfileAccessMode.ReadWrite))]);
        var fixture = Path.Combine(AppContext.BaseDirectory, "UpdateFixture", "QueueLoom.UpdateFixture" +
            (OperatingSystem.IsWindows() ? ".exe" : string.Empty));
        var go = Path.Combine(root, "go");
        var first = Path.Combine(root, "first"); var second = Path.Combine(root, "second");
        using var a = Start(first); using var b = Start(second);
        try
        {
            var clock = Stopwatch.StartNew();
            while(!File.Exists(first + ".ready") || !File.Exists(second + ".ready"))
            {
                Assert.False(a.HasExited || b.HasExited, "A claim process exited before the barrier.");
                Assert.True(clock.Elapsed < TimeSpan.FromSeconds(15), "Claim barrier timed out.");
                await Task.Delay(25);
            }
            File.WriteAllText(go, "go");
            await Task.WhenAll(a.WaitForExitAsync(), b.WaitForExitAsync()).WaitAsync(TimeSpan.FromSeconds(15));
            Assert.Equal(0, a.ExitCode); Assert.Equal(0, b.ExitCode);
            var claimed = new[] { File.ReadAllText(first + ".result"), File.ReadAllText(second + ".result") };
            Assert.Single(claimed, value => value == "True");
            Assert.Single(claimed, value => value == "False");
            Assert.Empty(store.Load());
        }
        finally
        {
            foreach(var child in new[] { a, b }) if(!child.HasExited) { child.Kill(); await child.WaitForExitAsync(); }
        }
        Process Start(string output)
        {
            var start = new ProcessStartInfo(fixture) { UseShellExecute = false, WindowStyle = ProcessWindowStyle.Hidden };
            foreach(var argument in new[] { "--claim-schedule", root, output, go }) start.ArgumentList.Add(argument);
            return Process.Start(start)!;
        }
    }
}
