using QueueLoom.Core.Updates;

namespace QueueLoom.Tests;

public sealed class StartupAcknowledgementTests
{
    [Fact]
    public async Task ABusyFileIsRetriedUntilTheAcknowledgementSucceeds()
    {
        var calls = 0;
        var failures = new List<Exception>();
        var result = await StartupAcknowledgement.RunAsync(() =>
        {
            if (++calls < 3) throw new InstallationFileBusyException("state.json", new IOException("sharing violation"));
            return true;
        }, failures.Add);

        Assert.True(result);
        Assert.Equal(3, calls);
        Assert.Empty(failures);
    }

    [Fact]
    public async Task AContendedLockIsRetried()
    {
        var calls = 0;
        var result = await StartupAcknowledgement.RunAsync(() => ++calls > 1 ? true : throw new IOException("lock"), _ => { });
        Assert.True(result);
    }

    [Fact]
    public async Task AMismatchIsReportedNotThrownAndNotRetried()
    {
        var calls = 0;
        var failures = new List<Exception>();
        var result = await StartupAcknowledgement.RunAsync(() => { calls++; throw new InvalidDataException("mismatch"); }, failures.Add);

        Assert.False(result);
        Assert.Equal(1, calls);
        Assert.IsType<InvalidDataException>(Assert.Single(failures));
    }

    [Fact]
    public async Task APersistentlyBusyFileIsReportedOnceTheBudgetIsSpent()
    {
        var failures = new List<Exception>();
        var result = await StartupAcknowledgement.RunAsync(() => throw new IOException("busy"), failures.Add, budget: TimeSpan.FromMilliseconds(600));

        Assert.False(result);
        Assert.IsType<IOException>(Assert.Single(failures));
    }

    [Fact]
    public async Task TheAcknowledgementDoesNotRunOnTheCallingThread()
    {
        // The caller is a dedicated non-pool thread that must get control back while the acknowledgement is still blocked.
        using var gate = new ManualResetEventSlim();
        using var started = new ManualResetEventSlim();
        var callerId = -1;
        var ranId = -1;
        Task<bool>? pending = null;
        var caller = new Thread(() =>
        {
            callerId = Environment.CurrentManagedThreadId;
            pending = StartupAcknowledgement.RunAsync(() =>
            {
                ranId = Environment.CurrentManagedThreadId;
                started.Set();
                gate.Wait();
                return true;
            }, _ => { });
        });
        caller.Start();
        Assert.True(caller.Join(TimeSpan.FromSeconds(10)), "RunAsync blocked its caller.");
        Assert.True(started.Wait(TimeSpan.FromSeconds(10)));
        Assert.False(pending!.IsCompleted);
        Assert.NotEqual(callerId, ranId);

        gate.Set();
        Assert.True(await pending);
    }

    [Fact]
    public async Task NoRetryStartsAfterTheCutoffEvenWhenOneAttemptWasSlow()
    {
        var clock = new SteppedClock();
        var calls = 0;
        var failures = new List<Exception>();
        var result = await StartupAcknowledgement.RunAsync(() =>
        {
            calls++;
            clock.Advance(TimeSpan.FromSeconds(30)); // A full installation-lock wait.
            throw new IOException("lock");
        }, failures.Add, clock);

        Assert.False(result);
        Assert.Equal(1, calls);
        Assert.Single(failures);
    }

    [Fact]
    public async Task NoRetryStartsWhenTheRetryDelayOvershootsTheCutoff()
    {
        var clock = new SteppedClock();
        var calls = 0;
        var failures = new List<Exception>();
        var run = StartupAcknowledgement.RunAsync(() =>
        {
            calls++;
            clock.Advance(TimeSpan.FromSeconds(19)); // Fails just before the 20 s cutoff, so a retry delay is allowed.
            throw new IOException("busy");
        }, failures.Add, clock);

        Assert.True(clock.TimerCreated.Wait(TimeSpan.FromSeconds(10)));
        clock.Advance(TimeSpan.FromSeconds(2)); // The delay resumes past the cutoff.
        clock.FireTimers();

        Assert.False(await run);
        Assert.Equal(1, calls);
        Assert.IsType<IOException>(Assert.Single(failures));
    }

    private sealed class SteppedClock : TimeProvider
    {
        private readonly List<(TimerCallback Callback, object? State)> _timers = [];
        private long _ticks;
        public SemaphoreSlim TimerCreated { get; } = new(0);
        public void Advance(TimeSpan by) => Interlocked.Add(ref _ticks, by.Ticks);
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
        public override long GetTimestamp() => Interlocked.Read(ref _ticks);

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            lock (_timers) _timers.Add((callback, state));
            TimerCreated.Release();
            return new NoTimer();
        }

        public void FireTimers()
        {
            (TimerCallback Callback, object? State)[] due;
            lock (_timers) { due = [.. _timers]; _timers.Clear(); }
            foreach (var (callback, state) in due) callback(state);
        }

        private sealed class NoTimer : ITimer
        {
            public bool Change(TimeSpan dueTime, TimeSpan period) => true;
            public void Dispose() { }
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }
}
