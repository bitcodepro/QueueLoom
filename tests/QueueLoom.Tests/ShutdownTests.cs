using QueueLoom.Core.Profiles;

namespace QueueLoom.Tests;

public sealed partial class ViewModelStateTests
{
    private sealed class SteppedTime : TimeProvider
    {
        private readonly object _sync = new();
        private readonly List<SteppedTimer> _timers = [];
        private DateTimeOffset _now = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() { lock (_sync) return _now; }

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new SteppedTimer(this, callback, state);
            timer.Change(dueTime, period);
            return timer;
        }

        public void Advance(TimeSpan by)
        {
            SteppedTimer[] due;
            lock (_sync)
            {
                _now += by;
                due = _timers.Where(timer => timer.Due is { } at && at <= _now).ToArray();
                foreach (var timer in due) timer.Due = null;
            }
            foreach (var timer in due) timer.Fire();
        }

        private sealed class SteppedTimer(SteppedTime owner, TimerCallback callback, object? state) : ITimer
        {
            public DateTimeOffset? Due { get; set; }
            public void Fire() => callback(state);

            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                lock (owner._sync)
                {
                    owner._timers.Remove(this);
                    Due = dueTime == Timeout.InfiniteTimeSpan ? null : owner._now + dueTime;
                    if (Due is not null) owner._timers.Add(this);
                }
                return true;
            }

            public void Dispose() { lock (owner._sync) owner._timers.Remove(this); Due = null; }
            public ValueTask DisposeAsync() { Dispose(); return ValueTask.CompletedTask; }
        }
    }

    private static async Task AdvanceUntilAsync(SteppedTime time, Task task)
    {
        for (var i = 0; i < 200 && !task.IsCompleted; i++)
        {
            time.Advance(TimeSpan.FromMinutes(1));
            await Task.WhenAny(task, Task.Delay(50));
        }
    }

    [Fact]
    public async Task ClosingFinishesWhenABrokerCallIgnoresCancellation()
    {
        var (viewModel, workspace, _) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        var time = new SteppedTime();
        viewModel.Clock = time;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var never = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        workspace.SearchGate = _ =>
        {
            started.TrySetResult();
            return never.Task; // a hung SDK call that does not observe the token
        };
        var search = viewModel.SearchDeadLettersCommand.ExecuteAsync();
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var closing = viewModel.DisposeAsync().AsTask();
        Assert.False(closing.IsCompleted);
        await AdvanceUntilAsync(time, closing);

        try
        {
            Assert.True(closing.IsCompleted);
            await closing;
            // The workspace is still in use by the hung call, so it is not disposed under it.
            Assert.Equal(0, workspace.DisposeCalls);
        }
        finally
        {
            never.TrySetResult();
            await Task.WhenAny(search, Task.Delay(TimeSpan.FromSeconds(10)));
        }
        for (var i = 0; i < 100 && workspace.DisposeCalls == 0; i++) await Task.Delay(50);
        Assert.Equal(1, workspace.DisposeCalls);
    }

    [Fact]
    public async Task ClosingFinishesWhenReleasingTheWorkspaceHangsOrFails()
    {
        var (viewModel, workspace, _) = await CreateSearchedViewModelAsync(ProfileAccessMode.ReadWrite);
        var time = new SteppedTime();
        viewModel.Clock = time;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        workspace.DisposeGate = () => new ValueTask(release.Task); // e.g. waiting for a gate a stuck call still holds

        var closing = viewModel.DisposeAsync().AsTask();
        await AdvanceUntilAsync(time, closing);
        Assert.True(closing.IsCompleted);
        await closing;
        Assert.Equal(1, workspace.DisposeCalls);

        // A late disposal fault is observed, not left unobserved.
        release.SetException(new InvalidOperationException("dispose failed"));
        await Task.Delay(100);
    }
}
