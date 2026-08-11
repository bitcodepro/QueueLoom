using QueueLoom.App.Commands;

namespace QueueLoom.Tests;

public sealed class AsyncRelayCommandTests
{
    [Fact]
    public async Task DisposeDuringExecution_CancelsAndCompletesWithoutLifetimeRace()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var command = new AsyncRelayCommand(async cancellationToken =>
        {
            started.SetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        });

        var completion = command.ExecuteAsync();
        await started.Task;
        command.Dispose();

        await completion;
        Assert.False(command.IsRunning);
        Assert.False(command.CanExecute(null));
    }
}
