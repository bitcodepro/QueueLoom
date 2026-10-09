using System.Diagnostics;

namespace QueueLoom.Core.Updates;

/// <summary>Waits for the application that asked the stable launcher to restart it.</summary>
public static class RestartHandoff
{
    public static readonly TimeSpan ExitWait = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Waits up to <paramref name="timeout"/> for the parent to exit. Versions are immutable and several instances
    /// may run side by side, so a parent that is slow to close (or a reused Unix PID) only delays the restart;
    /// it never cancels it.
    /// </summary>
    /// <returns>True when the parent is gone, false when the wait ran out and the restart proceeds anyway.</returns>
    public static async Task<bool> WaitForParentAsync(int processId, long startTicks, TimeSpan? timeout = null)
    {
        try
        {
            using var parent = Process.GetProcessById(processId);
            if (OperatingSystem.IsWindows() && parent.StartTime.ToUniversalTime().Ticks != startTicks) return true;
            await parent.WaitForExitAsync().WaitAsync(timeout ?? ExitWait).ConfigureAwait(false);
            return true;
        }
        catch (ArgumentException) { return true; /* Already stopped. */ }
        catch (TimeoutException) { return false; }
    }
}
