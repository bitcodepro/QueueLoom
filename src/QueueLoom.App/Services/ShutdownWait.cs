using Microsoft.Extensions.Logging;

namespace QueueLoom.App.Services;

/// <summary>
/// Bounds a shutdown step that may wait for a broker call ignoring cancellation (the service provider disposes the
/// workspace, which first waits for its running operations). The step keeps running and is observed; the window closes.
/// </summary>
internal static class ShutdownWait
{
    /// <returns>True when the step finished within <paramref name="timeout"/>.</returns>
    public static async Task<bool> WithinAsync(Func<ValueTask> step, TimeSpan timeout, TimeProvider clock, ILogger? logger)
    {
        Task running;
        try
        {
            running = step().AsTask();
        }
        catch (Exception exception)
        {
            logger?.LogWarning(exception, "Releasing application services failed");
            return true;
        }
        var finished = await Task.WhenAny(running, Task.Delay(timeout, clock)).ConfigureAwait(true);
        if (!ReferenceEquals(finished, running))
        {
            logger?.LogWarning("Releasing application services did not finish within {Timeout}; closing anyway", timeout);
            _ = running.ContinueWith(task => logger?.LogWarning(task.Exception, "Releasing application services failed"),
                CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
            return false;
        }
        try
        {
            await running.ConfigureAwait(true);
        }
        catch (Exception exception)
        {
            logger?.LogWarning(exception, "Releasing application services failed");
        }
        return true;
    }
}
