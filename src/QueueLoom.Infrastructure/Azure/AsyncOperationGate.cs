namespace QueueLoom.Infrastructure.Azure;

/// <summary>
/// Allows concurrent operations while giving lifecycle changes exclusive access
/// after every operation that entered before them has completed.
/// </summary>
internal sealed class AsyncOperationGate
{
    private readonly SemaphoreSlim _admission = new(1, 1);
    private readonly object _sync = new();
    private int _activeOperations;
    private TaskCompletionSource? _operationsDrained;

    public async ValueTask<IDisposable> EnterOperationAsync(
        CancellationToken cancellationToken = default)
    {
        await _admission.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            lock (_sync)
            {
                if (_activeOperations == 0)
                {
                    _operationsDrained = new TaskCompletionSource(
                        TaskCreationOptions.RunContinuationsAsynchronously);
                }

                _activeOperations++;
            }

            return new Lease(ExitOperation);
        }
        finally
        {
            _admission.Release();
        }
    }

    public async ValueTask<IDisposable> EnterLifecycleAsync(
        CancellationToken cancellationToken = default)
    {
        await _admission.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            Task operationsDrained;
            lock (_sync)
            {
                operationsDrained = _activeOperations == 0
                    ? Task.CompletedTask
                    : _operationsDrained!.Task;
            }

            await operationsDrained.WaitAsync(cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            return new Lease(() => _admission.Release());
        }
        catch
        {
            _admission.Release();
            throw;
        }
    }

    private void ExitOperation()
    {
        TaskCompletionSource? operationsDrained = null;
        lock (_sync)
        {
            _activeOperations--;
            if (_activeOperations == 0)
            {
                operationsDrained = _operationsDrained;
                _operationsDrained = null;
            }
        }

        operationsDrained?.TrySetResult();
    }

    private sealed class Lease(Action release) : IDisposable
    {
        private Action? _release = release;

        public void Dispose() => Interlocked.Exchange(ref _release, null)?.Invoke();
    }
}
