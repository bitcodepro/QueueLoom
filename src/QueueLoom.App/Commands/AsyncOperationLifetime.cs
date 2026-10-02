namespace QueueLoom.App.Commands;

/// <summary>Tracks complete operation scopes, including work started directly during initialization.</summary>
internal sealed class AsyncOperationLifetime
{
    private readonly object _sync = new();
    private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _active;
    private bool _stopped;

    public IDisposable? TryEnter()
    {
        lock (_sync)
        {
            if (_stopped) return null;
            _active++;
            return new Lease(this);
        }
    }

    public Task StopAndDrainAsync()
    {
        lock (_sync)
        {
            _stopped = true;
            if (_active == 0) _drained.TrySetResult();
            return _drained.Task;
        }
    }

    private void Exit()
    {
        lock (_sync)
            if (--_active == 0 && _stopped) _drained.TrySetResult();
    }

    private sealed class Lease(AsyncOperationLifetime owner) : IDisposable
    {
        private AsyncOperationLifetime? _owner = owner;
        public void Dispose() => Interlocked.Exchange(ref _owner, null)?.Exit();
    }
}
