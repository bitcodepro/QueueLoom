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

    // A resumed plan whose last item was already sent (through operation history) finishes with item 0: a
    // cancellation on that last acknowledgement must not stop it at the already-sent item that follows.
    [Fact]
    public async Task AResumedReplayCancelledAfterItsLastPendingSendIsCompleted()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new BatchReplayStore(directory.Path);
        var plan = await PrepareReplayRegression(store, profile);
        var workspace = new FakeWorkspace();
        await workspace.ConnectAsync(profile);
        await store.RunItemsAsync(plan, [1], false, workspace, () => true, null, default);
        using var cancellation = new CancellationTokenSource();
        var progress = new Immediate(report => { if (report.Sent == report.Total) cancellation.Cancel(); });

        var result = await store.RunAsync(plan, workspace, () => true, progress, cancellation.Token);

        Assert.Equal("Completed", result.Status);
        Assert.Equal(2, workspace.SentMessages.Count);
    }

    // Cancelling while work remains still stops the replay.
    [Fact]
    public async Task AReplayCancelledWithItemsLeftStops()
    {
        using var directory = new TemporaryDirectory();
        var profile = CreateProfile("Test", EnvironmentKind.Test, ProfileAccessMode.ReadWrite);
        var store = new BatchReplayStore(directory.Path);
        var plan = await PrepareReplayRegression(store, profile);
        var workspace = new FakeWorkspace();
        await workspace.ConnectAsync(profile);
        using var cancellation = new CancellationTokenSource();
        var progress = new Immediate(report => { if (report.Sent == 1) cancellation.Cancel(); });

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.RunAsync(plan, workspace, () => true, progress, cancellation.Token));

        Assert.Single(workspace.SentMessages);
    }

    private sealed class Immediate(Action<ReplayProgress> report) : IProgress<ReplayProgress>
    {
        public void Report(ReplayProgress value) => report(value);
    }
}
