using System.Diagnostics;

namespace QueueLoom.Core.Updates;

/// <summary>The restart launcher's wait for the application that handed the restart over to close.</summary>
public static class ApplicationExitWait
{
    /// <summary>Long enough for an orderly close that writes a large backup.</summary>
    public static readonly TimeSpan DefaultLimit = TimeSpan.FromMinutes(30);

    /// <summary>Start times that differ by less than this are the same process (systems round them differently).</summary>
    private static readonly TimeSpan StartTolerance = TimeSpan.FromSeconds(1);

    public enum Result
    {
        /// <summary>The application closed, or the PID now belongs to another process.</summary>
        Exited,

        /// <summary>The application was still running when the limit ran out.</summary>
        StillRunning
    }

    public static async Task<Result> WaitAsync(int processId, long startTicksUtc, TimeSpan limit)
    {
        Process process;
        try
        {
            process = Process.GetProcessById(processId);
        }
        catch (ArgumentException)
        {
            return Result.Exited;
        }

        using (process)
        {
            try
            {
                var started = process.StartTime.ToUniversalTime().Ticks;
                if (Math.Abs(started - startTicksUtc) >= StartTolerance.Ticks)
                {
                    return Result.Exited; // The PID was reused by another process.
                }
                await process.WaitForExitAsync().WaitAsync(limit).ConfigureAwait(false);
                return Result.Exited;
            }
            catch (TimeoutException)
            {
                return Result.StillRunning;
            }
            catch (InvalidOperationException)
            {
                return Result.Exited; // It exited between the lookup and the start-time read.
            }
        }
    }
}
