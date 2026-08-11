using QueueLoom.Infrastructure.Azure;

namespace QueueLoom.Tests.Infrastructure;

public sealed class AsyncOperationGateTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task OperationLeases_CanRunConcurrently()
    {
        var gate = new AsyncOperationGate();

        using var first = await gate.EnterOperationAsync();
        using var second = await gate.EnterOperationAsync().AsTask().WaitAsync(TestTimeout);
    }

    [Fact]
    public async Task LifecycleLease_WaitsForOperations_AndBlocksNewOperations()
    {
        var gate = new AsyncOperationGate();
        var activeOperation = await gate.EnterOperationAsync();

        var lifecycleTask = gate.EnterLifecycleAsync().AsTask();
        Assert.False(lifecycleTask.IsCompleted);

        activeOperation.Dispose();
        var lifecycle = await lifecycleTask.WaitAsync(TestTimeout);

        var blockedOperationTask = gate.EnterOperationAsync().AsTask();
        Assert.False(blockedOperationTask.IsCompleted);

        lifecycle.Dispose();
        using var resumedOperation = await blockedOperationTask.WaitAsync(TestTimeout);
    }

    [Fact]
    public async Task CancelledLifecycleWait_ReopensAdmissionForOperations()
    {
        var gate = new AsyncOperationGate();
        using var activeOperation = await gate.EnterOperationAsync();
        using var cancellation = new CancellationTokenSource();

        var lifecycleTask = gate.EnterLifecycleAsync(cancellation.Token).AsTask();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await lifecycleTask);

        using var concurrentOperation = await gate.EnterOperationAsync().AsTask().WaitAsync(TestTimeout);
    }
}
