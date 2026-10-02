using Microsoft.Extensions.Logging;

namespace QueueLoom.App.Services;

/// <summary>Diagnostic providers must not change an operation's outcome or break its error handler.</summary>
internal sealed class BestEffortLogger(ILogger inner) : ILogger
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(LogLevel level)
    {
        try { return inner.IsEnabled(level); }
        catch { return false; }
    }
    public void Log<TState>(LogLevel level, EventId id, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        try { inner.Log(level, id, state, exception, formatter); }
        catch { /* Drop failed diagnostics; never retry with unredacted state or exception text. */ }
    }
}
