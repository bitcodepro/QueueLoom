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

    /// <summary>What one look at the PID found.</summary>
    internal readonly record struct Reading(ReadingKind Kind, long Value = 0)
    {
        public static readonly Reading Gone = new(ReadingKind.Gone);
        public static readonly Reading Unreadable = new(ReadingKind.Unreadable);
        public static Reading Of(long value) => new(ReadingKind.Known, value);
    }

    internal enum ReadingKind
    {
        /// <summary>No process holds the PID: established, not assumed.</summary>
        Gone,

        /// <summary>A process holds the PID, but its identity could not be read (it may be exiting right now).</summary>
        Unreadable,

        /// <summary>The identity of the process holding the PID.</summary>
        Known
    }

    /// <summary>Takes the application's identity; call it while the application is known to be alive.</summary>
    public static Watch Identify(int processId, long startTicksUtc) =>
        Identify(processId, startTicksUtc, DefaultProbe(), OperatingSystem.IsWindows(), TimeProvider.System);

    /// <param name="comparable">
    /// Whether <paramref name="startTicksUtc"/> is directly comparable with the probe's identity (Windows' kernel
    /// creation time). Elsewhere the application's own reading cannot be compared, and the first identity read is used.
    /// </param>
    internal static Watch Identify(int processId, long startTicksUtc, IdentityProbe probe, bool comparable, TimeProvider clock)
    {
        var reading = Inspect(probe, processId);
        if (reading.Kind == ReadingKind.Gone ||
            reading.Kind == ReadingKind.Known && comparable && reading.Value != startTicksUtc)
        {
            return new Watch(processId, probe, clock, exited: true, identity: null, reference: null);
        }
        return new Watch(processId, probe, clock, exited: false,
            identity: reading.Kind == ReadingKind.Known ? reading.Value : null,
            reference: comparable ? startTicksUtc : null);
    }

    internal static IdentityProbe DefaultProbe() =>
        OperatingSystem.IsWindows() ? WindowsCreationTime
        : OperatingSystem.IsLinux() ? LinuxStartTicks
        : ExistsOnly;

    /// <summary>
    /// Looks at the PID once. A failed identity read is an exit only when a lookup then finds no process with the PID;
    /// a process that is present but unreadable is reported as such, never as gone.
    /// </summary>
    internal static Reading Inspect(IdentityProbe probe, int processId)
    {
        try
        {
            return probe(processId) is { } identity ? Reading.Of(identity) : Reading.Gone;
        }
        catch (Exception error) when (error is Win32Exception or InvalidOperationException or IOException or UnauthorizedAccessException)
        {
            return Exists(processId) ? Reading.Unreadable : Reading.Gone;
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

    /// <summary>
    /// A taken identity, waited on until it is gone. An identity not readable at first ("alive, unknown") is learned from
    /// the first successful read: regaining the ability to read it never counts as an exit. Only a confirmed absence, or
    /// a known identity replaced by another one, ends the wait.
    /// </summary>
    public sealed class Watch
    {
        private readonly int _processId;
        private readonly IdentityProbe _probe;
        private readonly TimeProvider _clock;
        private readonly bool _exited;
        private readonly long? _reference;
        private long? _identity;

        internal Watch(int processId, IdentityProbe probe, TimeProvider clock, bool exited, long? identity, long? reference)
        {
            _processId = processId;
            _probe = probe;
            _clock = clock;
            _exited = exited;
            _identity = identity;
            _reference = reference;
        }

        public async Task<Result> WaitAsync(TimeSpan limit)
        {
            if (_exited)
            {
                return Result.Exited;
            }
            var started = _clock.GetTimestamp();
            while (true)
            {
                var reading = Inspect(_probe, _processId);
                if (reading.Kind == ReadingKind.Gone)
                {
                    return Result.Exited;
                }
                if (reading.Kind == ReadingKind.Known)
                {
                    if (_identity is null)
                    {
                        // Learned only now. Where the application's own value is comparable, a different one is another
                        // process; elsewhere the reading becomes the identity.
                        if (_reference is { } reference && reading.Value != reference)
                        {
                            return Result.Exited;
                        }
                        _identity = reading.Value;
                    }
                    else if (reading.Value != _identity)
                    {
                        return Result.Exited; // Another process now holds the PID.
                    }
                }
                if (_clock.GetElapsedTime(started) >= limit)
                {
                    return Result.StillRunning;
                }
                await Task.Delay(PollInterval, _clock).ConfigureAwait(false);
            }
        }
    }
}
