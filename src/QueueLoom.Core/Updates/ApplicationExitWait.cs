using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;

namespace QueueLoom.Core.Updates;

/// <summary>
/// The restart launcher's wait for the application that handed the restart over to close. The application's identity
/// is taken while it is known to be alive (it waits for the launcher's readiness before closing), then checked until it
/// is gone or another process holds its PID:
/// <list type="bullet">
/// <item>Windows: the process creation time, a kernel value, compared with the one the application passed.</item>
/// <item>Linux: the start time in clock ticks since boot from /proc/&lt;pid&gt;/stat, which wall-clock changes do not
/// move (.NET's StartTime is rebuilt from the wall clock and is not a stable identity there).</item>
/// <item>Elsewhere: only whether the PID exists; when identity is uncertain the launcher keeps waiting.</item>
/// </list>
/// </summary>
public static class ApplicationExitWait
{
    /// <summary>Long enough for an orderly close that writes a large backup.</summary>
    public static readonly TimeSpan DefaultLimit = TimeSpan.FromMinutes(30);

    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(100);

    public enum Result
    {
        /// <summary>The application closed, or its PID now belongs to another process.</summary>
        Exited,

        /// <summary>The application was still running when the limit ran out.</summary>
        StillRunning
    }

    /// <summary>
    /// Reads the identity of the process with this PID: null when no such process exists. Throws when the process
    /// exists but could not be inspected.
    /// </summary>
    internal delegate long? IdentityProbe(int processId);

    /// <summary>Takes the application's identity; call it while the application is known to be alive.</summary>
    public static Watch Identify(int processId, long startTicksUtc)
    {
        var probe = DefaultProbe();
        var identity = Inspect(probe, processId, known: null);
        if (OperatingSystem.IsWindows() && identity is { } created && created != -1 && created != startTicksUtc)
        {
            identity = null; // Another process already holds the PID.
        }
        return new Watch(processId, identity, probe, TimeProvider.System);
    }

    internal static IdentityProbe DefaultProbe() =>
        OperatingSystem.IsWindows() ? WindowsCreationTime
        : OperatingSystem.IsLinux() ? LinuxStartTicks
        : ExistsOnly;

    /// <summary>
    /// The identity, or null when the process is gone. A process that exists but cannot be inspected (it may be exiting
    /// right now) keeps the identity already known: only an established exit counts as one.
    /// </summary>
    internal static long? Inspect(IdentityProbe probe, int processId, long? known)
    {
        try
        {
            return probe(processId);
        }
        catch (Exception error) when (error is Win32Exception or InvalidOperationException or IOException or UnauthorizedAccessException)
        {
            if (!Exists(processId))
            {
                return null;
            }
            // Alive but unreadable: when nothing is known yet, the PID itself stands in, so the wait goes on.
            return known ?? -1;
        }
    }

    private static bool Exists(int processId)
    {
        try
        {
            using var process = Process.GetProcessById(processId);
            return !process.HasExited;
        }
        catch (Exception error) when (error is ArgumentException or InvalidOperationException)
        {
            return false;
        }
        catch (Win32Exception)
        {
            return true; // Present, though not ours to read.
        }
    }

    private static long? WindowsCreationTime(int processId)
    {
        Process process;
        try
        {
            process = Process.GetProcessById(processId);
        }
        catch (ArgumentException)
        {
            return null;
        }
        using (process)
        {
            return process.StartTime.ToUniversalTime().Ticks;
        }
    }

    /// <summary>Field 22 of /proc/&lt;pid&gt;/stat: the start time in clock ticks after boot.</summary>
    internal static long? LinuxStartTicks(int processId)
    {
        string stat;
        try
        {
            stat = File.ReadAllText($"/proc/{processId.ToString(CultureInfo.InvariantCulture)}/stat");
        }
        catch (Exception error) when (error is FileNotFoundException or DirectoryNotFoundException)
        {
            return null;
        }
        // The command name (field 2) is parenthesised and may hold spaces or parentheses: fields follow the last ')'.
        var fields = stat[(stat.LastIndexOf(')') + 2)..].Split(' ');
        return long.Parse(fields[19], CultureInfo.InvariantCulture); // fields[0] is field 3
    }

    private static long? ExistsOnly(int processId) => Exists(processId) ? 0 : null;

    /// <summary>A taken identity, waited on until it is gone.</summary>
    public sealed class Watch
    {
        private readonly int _processId;
        private readonly long? _identity;
        private readonly IdentityProbe _probe;
        private readonly TimeProvider _clock;

        internal Watch(int processId, long? identity, IdentityProbe probe, TimeProvider clock)
        {
            _processId = processId;
            _identity = identity;
            _probe = probe;
            _clock = clock;
        }

        public async Task<Result> WaitAsync(TimeSpan limit)
        {
            if (_identity is not { } identity)
            {
                return Result.Exited;
            }
            var started = _clock.GetTimestamp();
            while (Inspect(_probe, _processId, identity) == identity)
            {
                if (_clock.GetElapsedTime(started) >= limit)
                {
                    return Result.StillRunning;
                }
                await Task.Delay(PollInterval, _clock).ConfigureAwait(false);
            }
            return Result.Exited;
        }
    }
}
