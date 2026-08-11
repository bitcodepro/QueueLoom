using System.Windows.Input;

namespace QueueLoom.App.Commands;

public sealed class AsyncRelayCommand(
    Func<CancellationToken, Task> execute,
    Func<bool>? canExecute = null) : ICommand, IDisposable
{
    private readonly object _sync = new();
    private CancellationTokenSource? _cancellation;
    private Task _completion = Task.CompletedTask;
    private bool _isRunning;
    private bool _isDisposed;

    public event EventHandler? CanExecuteChanged;

    public bool IsRunning
    {
        get
        {
            lock (_sync)
            {
                return _isRunning;
            }
        }
    }

    public Task Completion
    {
        get
        {
            lock (_sync)
            {
                return _completion;
            }
        }
    }

    public bool CanExecute(object? parameter)
    {
        lock (_sync)
        {
            return !_isDisposed && !_isRunning && (canExecute?.Invoke() ?? true);
        }
    }

    public async void Execute(object? parameter)
    {
        await ExecuteAsync(parameter).ConfigureAwait(true);
    }

    public Task ExecuteAsync(object? parameter = null)
    {
        CancellationTokenSource cancellation;
        lock (_sync)
        {
            if (_isDisposed || _isRunning || !(canExecute?.Invoke() ?? true))
            {
                return Task.CompletedTask;
            }

            _isRunning = true;
            cancellation = new CancellationTokenSource();
            _cancellation = cancellation;
        }

        var completion = ExecuteCoreAsync(cancellation);
        lock (_sync)
        {
            _completion = completion;
        }
        NotifyCanExecuteChanged();
        return completion;
    }

    private async Task ExecuteCoreAsync(CancellationTokenSource cancellation)
    {
        try
        {
            await execute(cancellation.Token).ConfigureAwait(true);
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
        {
        }
        finally
        {
            lock (_sync)
            {
                if (ReferenceEquals(_cancellation, cancellation))
                {
                    _cancellation = null;
                }
                _isRunning = false;
            }
            cancellation.Dispose();
            NotifyCanExecuteChanged();
        }
    }

    public void Cancel()
    {
        CancellationTokenSource? cancellation;
        lock (_sync)
        {
            cancellation = _cancellation;
        }

        try
        {
            cancellation?.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }
    }

    public void NotifyCanExecuteChanged() => CanExecuteChanged?.Invoke(this, EventArgs.Empty);

    public void Dispose()
    {
        lock (_sync)
        {
            _isDisposed = true;
        }
        Cancel();
        NotifyCanExecuteChanged();
    }
}
