namespace QueueLoom.Core.Updates;

/// <summary>
/// Confirms a launcher attempt away from the UI thread. A busy file or a contended installation lock is retried;
/// any other failure is reported, never thrown, so an unexpected error cannot crash a healthy payload into a rollback.
/// </summary>
/// <remarks>
/// <see cref="Budget"/> is a retry cutoff, not a bound on total duration: no new attempt starts once it has passed,
/// but an attempt already running may still wait up to 30 s for the installation lock and then hash the payload.
/// The cutoff is kept low so that last attempt can still finish inside the launcher's 60 s deadline.
/// </remarks>
public static class StartupAcknowledgement
{
    public static readonly TimeSpan Budget = TimeSpan.FromSeconds(20);
    public static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(500);

    /// <returns>The acknowledgement's result, or false when it could not be completed.</returns>
    public static async Task<bool> RunAsync(Func<bool> acknowledge, Action<Exception> failed,
        TimeProvider? time = null, TimeSpan? budget = null, CancellationToken cancellationToken = default)
    {
        time ??= TimeProvider.System;
        var limit = budget ?? Budget;
        var started = time.GetTimestamp();
        Exception? transient = null;
        while (true)
        {
            var expired = false;
            try
            {
                // The cutoff is checked where the attempt would begin: a delay or a queued worker can overshoot it.
                var result = await Task.Run(() =>
                {
                    if (transient is not null && time.GetElapsedTime(started) >= limit) { expired = true; return false; }
                    return acknowledge();
                }, cancellationToken).ConfigureAwait(false);
                if (!expired) return result;
                failed(transient!);
                return false;
            }
            catch (Exception error) when (error is IOException or InstallationFileBusyException &&
                time.GetElapsedTime(started) + RetryDelay < limit)
            {
                transient = error;
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
