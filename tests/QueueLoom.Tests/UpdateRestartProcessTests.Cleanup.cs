using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace QueueLoom.Tests;

/// <summary>
/// Cleanup of the isolated installation. The directory is removed only once every helper run a test started has
/// completed and every process running from the installation has been stopped and seen to exit. Anything less is a
/// cleanup failure: the installation is kept, and a failure of the test itself stays the one reported.
/// </summary>
public sealed partial class UpdateRestartProcessTests
{
    // Helper runs a test started without awaiting them to the end.
    private readonly List<Task> _helpers = [];
    private TimeSpan _helperBound = TimeSpan.FromSeconds(90);
    private static readonly TimeSpan ExitBound = TimeSpan.FromSeconds(10);

    /// <summary>Why the last cleanup could not prove completion; null after a cleanup that did.</summary>
    private Exception? _cleanupFailure;

    private Task<int> Own(Task<int> helper)
    {
        lock (_helpers) _helpers.Add(helper);
        return helper;
    }

    /// <summary>
    /// Runs a test body and then cleans up, whatever the body did. A failure of the body is what is thrown; a
    /// cleanup failure is thrown only when the body succeeded, and is kept in <see cref="_cleanupFailure"/> either way.
    /// </summary>
    private async Task IsolatedAsync(Func<Task> body)
    {
        var failed = false;
        try
        {
            await body();
        }
        catch
        {
            failed = true;
            throw;
        }
        finally
        {
            try { await StopOwnedAsync(); }
            catch (Exception) when (failed) { /* Kept in _cleanupFailure; the body's failure is the one to report. */ }
        }
    }

    private async Task StopOwnedAsync()
    {
        try
        {
            await StopOwnedCoreAsync();
            _cleanupFailure = null;
        }
        catch (Exception failure)
        {
            _cleanupFailure = failure;
            throw;
        }
    }

    private async Task StopOwnedCoreAsync()
    {
        Task[] helpers;
        lock (_helpers) helpers = [.. _helpers];
        foreach (var helper in helpers)
        {
            try { await helper.WaitAsync(_helperBound); }
            catch (TimeoutException)
            {
                // A timeout is not completion: the helper may still start or replace files in the installation.
                throw new InvalidOperationException($"A helper run was still pending after {_helperBound}; the installation is kept.");
            }
            catch (Exception) { /* Completed with a failure: its outcome belongs to the test. */ }
        }
        foreach (var process in _processes)
        {
            if (process.HasExited) continue;
            process.Kill();
            if (!process.WaitForExit(ExitBound)) throw new InvalidOperationException($"Process {process.Id} did not exit; the installation is kept.");
        }
        // The helper starts the restarted previous version without awaiting it. Each pass stops the processes found,
        // one by one; the last pass must find none.
        for (var pass = 0; pass < 10; pass++)
        {
            var owned = OwnedProcesses();
            if (owned.Count == 0) return;
            foreach (var process in owned)
            {
                using (process) process.Stop();
            }
        }
        var left = OwnedProcesses();
        if (left.Count > 0)
        {
            var names = string.Join(", ", left.Select(process => process.Id));
            left.ForEach(process => process.Dispose());
            throw new InvalidOperationException($"Processes of the installation were still running: {names}; the installation is kept.");
        }
    }

    /// <summary>
    /// Every process whose executable is in this test's own installation. Each is identified on the handle it is
    /// later stopped through, so a reused process ID can never be hit. A process whose executable cannot be read
    /// (for example one that has not finished loading) is not taken for absent: ownership that cannot be resolved
    /// fails the cleanup.
    /// </summary>
    private List<OwnedProcess> OwnedProcesses()
    {
        var root = Path.GetFullPath(_root) + Path.DirectorySeparatorChar;
        var owned = new List<OwnedProcess>();
        try
        {
            foreach (var candidate in Process.GetProcessesByName(Path.GetFileNameWithoutExtension(Fixture)))
            {
                using (candidate)
                {
                    if (OwnedProcess.Open(candidate, root) is { } process) owned.Add(process);
                }
            }
        }
        catch
        {
            owned.ForEach(process => process.Dispose());
            throw;
        }
        return owned;
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await StopOwnedAsync();
        }
        catch (Exception failure) when (TestContext.Current.TestState?.Result == TestResult.Failed)
        {
            // The test's own failure is reported; this one is only noted, and the installation is kept.
            TestContext.Current.TestOutputHelper?.WriteLine("Cleanup kept the test installation: " + failure.Message);
            return;
        }
        foreach (var process in _processes) process.Dispose();
        var cleanup = Stopwatch.StartNew();
        while (true)
        {
            try { Directory.Delete(_root, true); break; }
            catch (Exception exception) when ((exception is IOException or UnauthorizedAccessException) && cleanup.Elapsed < TimeSpan.FromSeconds(5))
            {
                // Windows may hold image sections briefly after the process exit signal.
                await Task.Delay(50);
            }
        }
    }

    /// <summary>A process of the installation, held by the handle it was identified on.</summary>
    private sealed class OwnedProcess : IDisposable
    {
        private readonly SafeProcessHandle? _handle;
        private readonly Process? _process;

        private OwnedProcess(int id, SafeProcessHandle? handle, Process? process)
        {
            Id = id;
            _handle = handle;
            _process = process;
        }

        public int Id { get; }

        /// <summary>The process if it runs from <paramref name="root"/>; null if it runs from elsewhere or has exited.</summary>
        public static OwnedProcess? Open(Process candidate, string root)
        {
            if (OperatingSystem.IsWindows())
            {
                var handle = OpenProcess(QueryLimitedInformation | Terminate | Synchronize, false, candidate.Id);
                if (handle.IsInvalid)
                {
                    var error = Marshal.GetLastWin32Error();
                    handle.Dispose();
                    if (error == InvalidParameter) return null; // Exited before it could be opened.
                    throw new InvalidOperationException($"Process {candidate.Id} could not be inspected (error {error}); its ownership is unknown.");
                }
                var path = new StringBuilder(32_768);
                var size = path.Capacity;
                if (!QueryFullProcessImageName(handle, 0, path, ref size))
                {
                    var error = Marshal.GetLastWin32Error();
                    // A process that is exiting can no longer report its executable before it is signalled as exited.
                    var exited = WaitForSingleObject(handle, (uint)ExitBound.TotalMilliseconds) == 0;
                    handle.Dispose();
                    if (exited) return null;
                    throw new InvalidOperationException($"The executable of process {candidate.Id} could not be read (error {error}); its ownership is unknown.");
                }
                if (path.ToString().StartsWith(root, StringComparison.OrdinalIgnoreCase)) return new OwnedProcess(candidate.Id, handle, null);
                handle.Dispose();
                return null;
            }

            Process process;
            try { process = Process.GetProcessById(candidate.Id); }
            catch (ArgumentException) { return null; }
            try
            {
                if (process.MainModule?.FileName is { } file && file.StartsWith(root, StringComparison.Ordinal)) return new OwnedProcess(candidate.Id, null, process);
            }
            catch (InvalidOperationException) { /* Exited meanwhile. */ }
            catch (Win32Exception error)
            {
                process.Dispose();
                throw new InvalidOperationException($"The executable of process {candidate.Id} could not be read ({error.Message}); its ownership is unknown.");
            }
            process.Dispose();
            return null;
        }

        /// <summary>Stops this process only, never its descendants, and fails unless it is seen to exit.</summary>
        public void Stop()
        {
            if (_handle is not null)
            {
                if (WaitForSingleObject(_handle, 0) == 0) return;
                // Termination is refused for a process that is already exiting; either way it must be seen to exit.
                var error = TerminateProcess(_handle, 1) ? 0 : Marshal.GetLastWin32Error();
                if (WaitForSingleObject(_handle, (uint)ExitBound.TotalMilliseconds) != 0)
                    throw new InvalidOperationException($"Process {Id} did not exit (termination error {error}); the installation is kept.");
                return;
            }
            if (_process!.HasExited) return;
            _process.Kill();
            if (!_process.WaitForExit(ExitBound)) throw new InvalidOperationException($"Process {Id} did not exit; the installation is kept.");
        }

        public void Dispose()
        {
            _handle?.Dispose();
            _process?.Dispose();
        }
    }

    private const uint QueryLimitedInformation = 0x1000, Terminate = 0x0001, Synchronize = 0x0010_0000;
    private const int InvalidParameter = 87;

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern SafeProcessHandle OpenProcess(uint access, [MarshalAs(UnmanagedType.Bool)] bool inherit, int id);

    [DllImport("kernel32.dll", EntryPoint = "QueryFullProcessImageNameW", CharSet = CharSet.Unicode, SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool QueryFullProcessImageName(SafeProcessHandle process, uint flags, StringBuilder name, ref int size);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool TerminateProcess(SafeProcessHandle process, uint exitCode);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern uint WaitForSingleObject(SafeProcessHandle handle, uint milliseconds);
}
