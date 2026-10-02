using System.Windows.Input;

namespace QueueLoom.App.Commands;

public sealed class AsyncRelayCommand(
    Func<CancellationToken, Task> execute,
    Func<bool>? canExecute = null) : ICommand, IAsyncCommandLifetime
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
        TaskCompletionSource completion;
        lock (_sync)
        {
            if (_isDisposed || _isRunning || !(canExecute?.Invoke() ?? true))
            {
                return Task.CompletedTask;
            }

            _isRunning = true;
            cancellation = new CancellationTokenSource();
            _cancellation = cancellation;
            completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _completion = completion.Task;
        }

        _ = CompleteAsync(cancellation, completion);
        NotifyCanExecuteChanged();
        return completion.Task;
    }

    private async Task CompleteAsync(CancellationTokenSource cancellation, TaskCompletionSource completion)
    {
        try { await ExecuteCoreAsync(cancellation).ConfigureAwait(true); completion.TrySetResult(); }
        catch (Exception error) { completion.TrySetException(error); }
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

/// <summary>An asynchronous command with a parameter, for actions on a row (a rule, a subscription). It never runs twice at once.</summary>
public sealed class AsyncRelayCommand<T>(
    Func<T?, CancellationToken, Task> execute,
    Predicate<T?>? canExecute = null) : ICommand, IAsyncCommandLifetime
{
    private readonly object _sync = new();
    private bool _isRunning;
    private bool _isDisposed;
    private CancellationTokenSource? _cancellation;
    private Task _completion = Task.CompletedTask;

    public event EventHandler? CanExecuteChanged;

    public Task Completion { get { lock (_sync) return _completion; } }

    public bool CanExecute(object? parameter)
    {
        lock (_sync) return !_isDisposed && !_isRunning && (canExecute?.Invoke(parameter is T value ? value : default) ?? true);
    }

    public async void Execute(object? parameter) => await ExecuteAsync(parameter is T value ? value : default).ConfigureAwait(true);

    public Task ExecuteAsync(T? parameter)
    {
        CancellationTokenSource cancellation;
        TaskCompletionSource completion;
        lock (_sync)
        {
            if (_isDisposed || _isRunning || !(canExecute?.Invoke(parameter) ?? true)) return Task.CompletedTask;
            _isRunning = true;
            _cancellation = cancellation = new CancellationTokenSource();
            completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _completion = completion.Task;
        }
        _ = CompleteAsync(parameter, cancellation, completion);
        NotifyCanExecuteChanged();
        return completion.Task;
    }

    private async Task CompleteAsync(T? parameter, CancellationTokenSource cancellation, TaskCompletionSource completion)
    {
        Exception? error = null;
        try
        {
            await execute(parameter, cancellation.Token).ConfigureAwait(true);
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
        catch (Exception exception) { error = exception; }
        finally
        {
            lock (_sync) { _isRunning = false; _cancellation = null; }
            cancellation.Dispose();
            NotifyCanExecuteChanged();
        }
        if (error is null) completion.TrySetResult();
        else completion.TrySetException(error);
    }

    public void Cancel()
    {
        CancellationTokenSource? cancellation;
        lock (_sync) cancellation = _cancellation;
        try { cancellation?.Cancel(); }
        catch (ObjectDisposedException) { }
    }

    public void Dispose()
    {
        lock (_sync) _isDisposed = true;
        Cancel();
        NotifyCanExecuteChanged();
    }

    public void NotifyCanExecuteChanged() => CanExecuteChanged?.Invoke(this, EventArgs.Empty);
}
