using QueueLoom.Core.Profiles;
using QueueLoom.Core.ServiceBus;
using QueueLoom.Infrastructure.Persistence;
using QueueLoom.Tests.Infrastructure;

namespace QueueLoom.Tests;

// Review of the replay pacing: it waited after the last send too, so a cancellation during that wait reported a batch
// whose every copy was acknowledged as stopped.
public sealed partial class ViewModelStateTests
{
    [Fact]
    public async Task AReplayCancelledAfterItsLastSendIsCompleted()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var delays = 0;
        var store = new BatchReplayStore(directory.Path) { DelayAsync = (delay, token) => { delays++; return Task.Delay(delay, token); } };
        var plan = await PrepareReplayRegression(store, profile);
        var workspace = new FakeWorkspace();
        await workspace.ConnectAsync(profile);
        using var cancellation = new CancellationTokenSource();
        var progress = new Immediate(report => { if (report.Sent == report.Total) cancellation.Cancel(); });

        var result = await store.RunAsync(plan, workspace, () => true, progress, cancellation.Token);

        Assert.Equal("Completed", result.Status);
        Assert.Equal(plan.Count, workspace.SentMessages.Count);
        Assert.Equal(plan.Count - 1, delays);
    }

    private sealed class Immediate(Action<ReplayProgress> report) : IProgress<ReplayProgress>
    {
        public void Report(ReplayProgress value) => report(value);
    }
}
