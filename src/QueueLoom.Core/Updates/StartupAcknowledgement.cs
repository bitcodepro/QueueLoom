namespace QueueLoom.Core.Updates;

/// <summary>
/// Confirms a launcher attempt away from the UI thread. A busy file or a contended installation lock is retried
/// within <see cref="Budget"/>, which stays well inside the launcher's 60 s deadline; any other failure is reported,
/// never thrown, so an unexpected error cannot crash a healthy payload into a rollback.
/// </summary>
public static class StartupAcknowledgement
{
    public static readonly TimeSpan Budget = TimeSpan.FromSeconds(40);
    public static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(500);

    /// <returns>The acknowledgement's result, or false when it could not be completed.</returns>
    public static async Task<bool> RunAsync(Func<bool> acknowledge, Action<Exception> failed,
        TimeProvider? time = null, TimeSpan? budget = null, CancellationToken cancellationToken = default)
    {
        time ??= TimeProvider.System;
        var started = time.GetTimestamp();
        while (true)
        {
            try
            {
                return await Task.Run(acknowledge, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception error) when (error is IOException or InstallationFileBusyException &&
                time.GetElapsedTime(started) + RetryDelay < (budget ?? Budget))
            {
                await Task.Delay(RetryDelay, time, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception error) when (error is not OutOfMemoryException)
            {
                failed(error);
                return false;
            }
        }
    }
}
