namespace QueueLoom.App.Commands;

internal interface IAsyncCommandLifetime : IDisposable
{
    Task Completion { get; }
}

/// <summary>Owns every asynchronous command of a view model, including commands added by partial features.</summary>
internal sealed class AsyncCommandLifetime
{
    private readonly List<IAsyncCommandLifetime> _commands = [];
    private bool _stopped;

    public AsyncRelayCommand Create(Func<CancellationToken, Task> execute, Func<bool>? canExecute = null)
    {
        var command = new AsyncRelayCommand(execute, canExecute);
        if (_stopped) command.Dispose();
        _commands.Add(command);
        return command;
    }

    public AsyncRelayCommand<T> Create<T>(Func<T?, CancellationToken, Task> execute, Predicate<T?>? canExecute = null)
    {
        var command = new AsyncRelayCommand<T>(execute, canExecute);
        if (_stopped) command.Dispose();
        _commands.Add(command);
        return command;
    }

    public Task StopAndDrainAsync()
    {
        _stopped = true;
        // Dispose blocks admission before cancellation is delivered. Snapshot Completion
        // only after every command has stopped accepting work.
        foreach (var command in _commands) command.Dispose();
        return Task.WhenAll(_commands.Select(command => command.Completion));
    }
}
