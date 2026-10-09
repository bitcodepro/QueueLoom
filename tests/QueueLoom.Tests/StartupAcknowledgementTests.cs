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
        var caller = Environment.CurrentManagedThreadId;
        var ran = -1;
        await StartupAcknowledgement.RunAsync(() => { ran = Environment.CurrentManagedThreadId; return true; }, _ => { });
        Assert.NotEqual(caller, ran);
    }
}
